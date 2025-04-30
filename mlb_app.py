import streamlit as st
import pymysql, re
from collections import defaultdict
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# ────────── PAGE CONFIG ──────────
st.set_page_config(page_title="NBA Futures Dashboard", layout="wide")

# ────────── DB HELPERS ──────────

def new_betting_conn():
    return pymysql.connect(
        host=st.secrets["BETTING_DB"]["host"],
        user=st.secrets["BETTING_DB"]["user"],
        password=st.secrets["BETTING_DB"]["password"],
        database=st.secrets["BETTING_DB"]["database"],
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )

def new_futures_conn():
    return pymysql.connect(
        host=st.secrets["FUTURES_DB"]["host"],
        user=st.secrets["FUTURES_DB"]["user"],
        password=st.secrets["FUTURES_DB"]["password"],
        database=st.secrets["FUTURES_DB"]["database"],
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )

def with_cursor(conn):
    conn.ping(reconnect=True)
    return conn.cursor()

# ────────── ODDS HELPERS ──────────

def american_odds_to_decimal(o:int)->float:
    return 1 + (o/100) if o>0 else 1 + 100/abs(o) if o else 1

def american_odds_to_prob(o:int)->float:
    return 100/(o+100) if o>0 else abs(o)/(abs(o)+100) if o else 0

def cast_odds(v):
    if v in (None, "", 0):
        return 0
    if isinstance(v, (int, float)):
        return int(v)
    m = re.search(r"[-+]?\d+", str(v))
    return int(m.group()) if m else 0

# ────────── MAPS ──────────

futures_table_map = {
    ("Championship", "NBA Championship"): "NBAChampionship",
    ("Conference Winner", "Eastern Conference"): "NBAEasternConference",
    ("Conference Winner", "Western Conference"): "NBAWesternConference",
    ("Defensive Player of Year Award", "Award"): "NBADefensivePotY",
    ("Division Winner", "Atlantic Division"): "NBAAtlantic",
    ("Division Winner", "Central Division"): "NBACentral",
    ("Division Winner", "Northwest Division"): "NBANorthwest",
    ("Division Winner", "Pacific Division"): "NBAPacific",
    ("Division Winner", "Southeast Division"): "NBASoutheast",
    ("Division Winner", "Southwest Division"): "NBASouthwest",
    ("Most Improved Player Award", "Award"): "NBAMIP",
    ("Most Valuable Player Award", "Award"): "NBAMVP",
    ("Rookie of Year Award", "Award"): "NBARotY",
    ("Sixth Man of Year Award", "Award"): "NBASixthMotY",
}

team_alias_map = {
    "Atlanta Hawks": "Hawks", "Boston Celtics": "Celtics", "Brooklyn Nets": "Nets",
    "Charlotte Hornets": "Hornets", "Chicago Bulls": "Bulls", "Cleveland Cavaliers": "Cavaliers",
    "Dallas Mavericks": "Mavericks", "Denver Nuggets": "Nuggets", "Detroit Pistons": "Pistons",
    "Golden State Warriors": "Warriors", "Houston Rockets": "Rockets", "Indiana Pacers": "Pacers",
    "LA Clippers": "Clippers", "Los Angeles Clippers": "Clippers", "Los Angeles Lakers": "Lakers",
    "Memphis Grizzlies": "Grizzlies", "Miami Heat": "Heat", "Milwaukee Bucks": "Bucks",
    "Minnesota Timberwolves": "Timberwolves", "New Orleans Pelicans": "Pelicans",
    "New York Knicks": "Knicks", "Oklahoma City Thunder": "Thunder", "Orlando Magic": "Magic",
    "Philadelphia 76ers": "76ers", "Phoenix Suns": "Suns", "Portland Trail Blazers": "Trail Blazers",
    "Sacramento Kings": "Kings", "San Antonio Spurs": "Spurs", "Toronto Raptors": "Raptors",
    "Utah Jazz": "Jazz", "Washington Wizards": "Wizards"
}

sportsbook_cols = [
    "BetMGM", "DraftKings", "Caesars", "ESPNBet", "FanDuel",
    "BallyBet", "RiversCasino", "Bet365",
]

# ────────── CORE ──────────

def best_odds_decimal_prob(event_type, event_label, participant, cutoff_dt, fut_conn):
    tbl = futures_table_map.get((event_type, event_label))
    if not tbl:
        return 1.0, 0.0
    alias = team_alias_map.get(participant, participant)
    with with_cursor(fut_conn) as cur:
        cur.execute(
            f"SELECT {','.join(sportsbook_cols)} FROM {tbl} WHERE team_name=%s AND date_created<=%s ORDER BY date_created DESC LIMIT 1",
            (alias, cutoff_dt),
        )
        row = cur.fetchone()
    if not row:
        return 1.0, 0.0
    nums = [cast_odds(row[c]) for c in sportsbook_cols if cast_odds(row[c])]
    if not nums:
        return 1.0, 0.0
    best = max(nums)
    return american_odds_to_decimal(best), american_odds_to_prob(best)

# ────────── EV TABLE (unchanged) ──────────
# ... existing build_ev_dataframe() and ev_table_page() definitions remain here (omitted for brevity) ...

# ────────── ODDS MOVEMENT PAGE ──────────

def odds_movement_page():
    st.header("Weekly Odds Movement (Top‑5)")

    fut_conn = new_futures_conn()

    # user selects market
    etypes = sorted({t for (t, _) in futures_table_map})
    sel_type = st.selectbox("Event Type", etypes, key="om_type")
    elabels = sorted({l for (t, l) in futures_table_map if t == sel_type})
    sel_lbl = st.selectbox("Event Label", elabels, key="om_lbl")

    col1, col2 = st.columns(2)
    sd = col1.date_input("Start", datetime.utcnow().date() - timedelta(days=120), key="om_sd")
    ed = col2.date_input("End", datetime.utcnow().date(), key="om_ed")
    if sd > ed:
        st.error("Start date must precede end date"); return

    if not st.button("Plot Odds Movement", key="plot_btn"):
        st.stop()

    tbl = futures_table_map[(sel_type, sel_lbl)]

    # pull odds snapshots in range
    with with_cursor(fut_conn) as cur:
        cur.execute(
            f"SELECT team_name, date_created, {','.join(sportsbook_cols)} FROM {tbl} "
            "WHERE date_created BETWEEN %s AND %s ORDER BY team_name, date_created",
            (f"{sd} 00:00:00", f"{ed} 23:59:59"),
        )
        raw = pd.DataFrame(cur.fetchall())

    if raw.empty:
        st.warning("No odds data for that range"); return

    # best odds ➜ prob
    raw[sportsbook_cols] = raw[sportsbook_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
    raw["best"] = raw[sportsbook_cols].replace(0, pd.NA).max(axis=1).fillna(0).astype(int)
    raw["prob"] = raw["best"].apply(american_odds_to_prob)
    raw["date"] = pd.to_datetime(raw["date_created"]).dt.date

    # keep last snapshot per team per day
    raw = (raw.sort_values(["team_name", "date"]).groupby(["team_name", "date"]).tail(1))[
        ["team_name", "date", "prob"]
    ]

    # resample to weekly (Monday start) – forward fill within each team
    raw["week"] = pd.to_datetime(raw["date"]).dt.to_period("W").dt.start_time
    weekly = (
        raw.sort_values(["team_name", "week"])
        .groupby("team_name")
        .apply(lambda g: g.drop_duplicates('week').set_index('week').asfreq('W-MON').ffill())
        .reset_index(level=0)
        .reset_index()
    )

    # pick top‑5 players/teams by prob on last week
    last_week = weekly["week"].max()
    top5_names = (
        weekly[weekly["week"] == last_week]
        .nlargest(5, "prob")["team_name"]
        .tolist()
    )
    top5 = weekly[weekly["team_name"].isin(top5_names)]

    # ---- plot ----
    fig, ax = plt.subplots(figsize=(11, 6))
    for name, grp in top5.groupby("team_name"):
        ax.plot(grp["week"], grp["prob"] * 100, marker="o", linewidth=2, label=name)

    ax.set_title(f"{sel_lbl} Implied Probability Over Time", fontsize=16, pad=12)
    ax.set_xlabel("Week", fontsize=12)
    ax.set_ylabel("Implied Probability (%)", fontsize=12)
    ax.yaxis.set_major_formatter(mdates.PercentFormatter())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    plt.xticks(rotation=45)
    ax.legend(title="Top‑5", bbox_to_anchor=(1.02, 1), loc="upper left")
    ax.grid(True, alpha=0.3)
    st.pyplot(fig, use_container_width=True)

# ────────── SIDEBAR NAV ──────────

page = st.sidebar.radio("Choose Page", ["EV Table", "Odds Movement Plot"], key="nav")

if page == "EV Table":
    # build_ev_dataframe & ev_table_page assumed present (omitted here for brevity)
    st.write("EV Table not included in this snippet")
else:
    odds_movement_page()
