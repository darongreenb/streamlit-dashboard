import streamlit as st
import pymysql
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta
from collections import defaultdict
import re

# ───────────────── DB HELPERS ─────────────────
def new_betting_conn():
    return pymysql.connect(
        host       = st.secrets["BETTING_DB"]["host"],
        user       = st.secrets["BETTING_DB"]["user"],
        password   = st.secrets["BETTING_DB"]["password"],
        database   = st.secrets["BETTING_DB"]["database"],
        cursorclass= pymysql.cursors.DictCursor,
        autocommit = True,
    )

def new_futures_conn():
    return pymysql.connect(
        host       = st.secrets["FUTURES_DB"]["host"],
        user       = st.secrets["FUTURES_DB"]["user"],
        password   = st.secrets["FUTURES_DB"]["password"],
        database   = st.secrets["FUTURES_DB"]["database"],
        cursorclass= pymysql.cursors.DictCursor,
        autocommit = True,
    )

def with_cursor(conn):
    conn.ping(reconnect=True)
    return conn.cursor()

# ───────────────── UTILS ─────────────────
def american_odds_to_prob(o:int)->float:
    return 100/(o+100) if o>0 else abs(o)/(abs(o)+100) if o else 0.0

def cast_odds(v):
    if v in (None, "", 0): return 0
    if isinstance(v, (int, float)): return int(v)
    m = re.search(r"[-+]?\d+", str(v))
    return int(m.group()) if m else 0

futures_table_map = {
    ("Most Valuable Player Award","Award"):  "NBAMVP",
    ("Championship","NBA Championship"): "NBAChampionship",
    ("Conference Winner","Eastern Conference"): "NBAEasternConference",
    ("Conference Winner","Western Conference"): "NBAWesternConference",
    ("Defensive Player of Year Award","Award"): "NBADefensivePotY",
    ("Division Winner","Atlantic Division"): "NBAAtlantic",
    ("Division Winner","Central Division"):  "NBACentral",
    ("Division Winner","Northwest Division"):"NBANorthwest",
    ("Division Winner","Pacific Division"):  "NBAPacific",
    ("Division Winner","Southeast Division"): "NBASoutheast",
    ("Division Winner","Southwest Division"): "NBASouthwest",
    ("Most Improved Player Award","Award"):  "NBAMIP",
    ("Rookie of Year Award","Award"):        "NBARotY",
    ("Sixth Man of Year Award","Award"):     "NBASixthMotY",
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

sportsbook_cols = ["BetMGM","DraftKings","Caesars","ESPNBet","FanDuel",
                   "BallyBet","RiversCasino","Bet365"]

# ───────────────── STREAMLIT PAGE ─────────────────
def return_plot_page():
    st.header("% Return Plot")
    fut_conn = new_futures_conn()
    bet_conn = new_betting_conn()

    ev_types = sorted({t for (t, _) in futures_table_map})
    sel_type = st.selectbox("Event Type", ev_types)
    labels   = sorted({lbl for (t, lbl) in futures_table_map if t == sel_type})
    sel_lbl  = st.selectbox("Event Label", labels)

    col1, col2 = st.columns(2)
    start_date = col1.date_input("Start", datetime.utcnow().date() - timedelta(days=60))
    end_date   = col2.date_input("End",   datetime.utcnow().date())
    if start_date > end_date:
        st.error("Start date must precede end date")
        return

    if not st.button("Generate Plot"):
        st.info("Choose filters & press **Generate Plot**")
        return

    with with_cursor(bet_conn) as cur:
        cur.execute("""
            SELECT b.WagerID, b.PotentialPayout, b.DollarsAtStake, b.DateTimePlaced,
                   l.EventType, l.EventLabel, l.ParticipantName
              FROM bets b JOIN legs l ON b.WagerID = l.WagerID
             WHERE b.WhichBankroll='GreenAleph' AND b.WLCA='Active' AND l.LeagueName='NBA'
        """)
        rows = cur.fetchall()
    bet_df = pd.DataFrame(rows)
    if bet_df.empty:
        st.warning("No active wagers found"); return

    tbl_name = futures_table_map[(sel_type, sel_lbl)]
    participants = bet_df[(bet_df.EventType == sel_type) & (bet_df.EventLabel == sel_lbl)]["ParticipantName"].map(lambda x: team_alias_map.get(x, x)).unique().tolist()

    with with_cursor(fut_conn) as cur:
        placeholders = ",".join(["%s"] * len(participants))
        cur.execute(
            f"""SELECT team_name, MAX(date_created) as date_created, {','.join(sportsbook_cols)}
                FROM {tbl_name}
                WHERE team_name IN ({placeholders})
                GROUP BY team_name""",
            participants
        )
        odds_data = pd.DataFrame(cur.fetchall())

    odds_map = {}
    for _, row in odds_data.iterrows():
        odds = [cast_odds(row[col]) for col in sportsbook_cols if cast_odds(row[col]) != 0]
        if odds:
            dec = 1 + (max(odds) / 100 if max(odds) > 0 else 100 / abs(max(odds)))
            odds_map[row["team_name"]] = dec

    records = []
    for wager_id, group in bet_df.groupby("WagerID"):
        legs = group[(group.EventType == sel_type) & (group.EventLabel == sel_lbl)]
        decs = []
        for _, row in legs.iterrows():
            alias = team_alias_map.get(row["ParticipantName"], row["ParticipantName"])
            dec = odds_map.get(alias, 1.0)
            if dec > 1.0:
                decs.append(dec)
        sum_exc = sum(d - 1 for d in decs)
        if sum_exc <= 0: continue
        for d in decs:
            records.append({"WagerID": wager_id, "Weight": (d - 1) / sum_exc})

    wgt = pd.DataFrame(records)
    if wgt.empty:
        st.warning("No relevant legs found"); return

    @st.cache_data(ttl=3600, show_spinner=False)
    def cached_odds(tbl, names, start, end):
        if not names: return pd.DataFrame()
        placeholders = ",".join(["%s"]*len(names))
        with with_cursor(fut_conn) as cur:
            cur.execute(
                f"""SELECT team_name,date_created,{','.join(sportsbook_cols)}
                      FROM {tbl}
                     WHERE team_name IN ({placeholders})
                       AND date_created BETWEEN %s AND %s
                     ORDER BY team_name,date_created""",
                (*names, f"{start} 00:00:00", f"{end} 23:59:59")
            )
            raw = pd.DataFrame(cur.fetchall())
        if raw.empty: return raw
        raw[sportsbook_cols] = raw[sportsbook_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
        raw["best"] = raw[sportsbook_cols].replace(0, pd.NA).max(axis=1).fillna(0).astype(int)
        raw["prob"] = raw["best"].apply(american_odds_to_prob)
        raw["date"] = pd.to_datetime(raw["date_created"]).dt.date
        return (raw.sort_values(["team_name","date"])
            .groupby(["team_name","date"])
            .tail(1)[["team_name", "date", "prob"]])


    odds_df = cached_odds(tbl_name, participants, start_date, end_date)
    if odds_df.empty:
        st.warning("No odds data found"); return

    bet_meta = bet_df[["WagerID","PotentialPayout","DollarsAtStake","DateTimePlaced"]].drop_duplicates()
    merged = (bet_df
              .merge(odds_df, how="left", left_on="ParticipantName", right_on="team_name")
              .dropna(subset=["prob"]))
    merged["date"] = pd.to_datetime(merged["date"])
    merged = merged.merge(wgt, on="WagerID", how="inner")

    daily = (merged.groupby(["date","WagerID"])
                   .agg(prob_product=("prob","prod"), weight=("Weight","first"))
                   .reset_index()
                   .merge(bet_meta, on="WagerID", how="left"))

    for col in ["weight","DollarsAtStake","PotentialPayout"]:
        daily[col] = pd.to_numeric(daily[col], errors="coerce").fillna(0.0)

    daily["stake_part"] = daily["DollarsAtStake"] * daily["weight"]
    daily["exp_part"]   = daily["PotentialPayout"] * daily["prob_product"] * daily["weight"]
    daily["net_part"]   = daily["exp_part"] - daily["stake_part"]

    series = (daily.groupby("date")
                   .agg(net=("net_part","sum"), stake=("stake_part","sum"))
                   .reset_index())
    series["pct"] = (series["net"] / series["stake"]) * 100
    series = series[(series["date"].dt.date >= start_date) & (series["date"].dt.date <= end_date)]

    if series.empty:
        st.warning("Insufficient data to plot"); return

    fig, ax = plt.subplots(figsize=(10,5))
    ax.plot(series["date"], series["pct"], marker="o")
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
    ax.set_title(f"% Return — {sel_type}: {sel_lbl}")
    ax.set_xlabel("Date"); ax.set_ylabel("Return (%)")
    plt.xticks(rotation=45)
    st.pyplot(fig, use_container_width=True)

# ───────────────── SIDEBAR NAV ─────────────────
page = st.sidebar.radio("Choose Page", ["% Return Plot"])
if page == "% Return Plot":
    return_plot_page()
