import streamlit as st
import pymysql, re
from collections import defaultdict
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import PercentFormatter

# ─────────────────── PAGE CONFIG ───────────────────
st.set_page_config(page_title="NBA Futures Dashboard", layout="wide")

# ─────────────────── DB HELPERS ───────────────────

def new_betting_conn():
    return pymysql.connect(**st.secrets["BETTING_DB"], cursorclass=pymysql.cursors.DictCursor, autocommit=True)

def new_futures_conn():
    return pymysql.connect(**st.secrets["FUTURES_DB"], cursorclass=pymysql.cursors.DictCursor, autocommit=True)

def with_cursor(conn):
    conn.ping(reconnect=True)
    return conn.cursor()

# ─────────────────── ODDS HELPERS ───────────────────

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

# ─────────────────── MAPPINGS ───────────────────

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

team_alias_map = {n:n.split()[-1] if n.startswith("Los") else n.split(maxsplit=1)[-1] for n in [
    "Atlanta Hawks","Boston Celtics","Brooklyn Nets","Charlotte Hornets","Chicago Bulls","Cleveland Cavaliers","Dallas Mavericks","Denver Nuggets","Detroit Pistons","Golden State Warriors","Houston Rockets","Indiana Pacers","LA Clippers","Los Angeles Clippers","Los Angeles Lakers","Memphis Grizzlies","Miami Heat","Milwaukee Bucks","Minnesota Timberwolves","New Orleans Pelicans","New York Knicks","Oklahoma City Thunder","Orlando Magic","Philadelphia 76ers","Phoenix Suns","Portland Trail Blazers","Sacramento Kings","San Antonio Spurs","Toronto Raptors","Utah Jazz","Washington Wizards"]}

sportsbook_cols = ["BetMGM","DraftKings","Caesars","ESPNBet","FanDuel","BallyBet","RiversCasino","Bet365"]

# ─────────────────── CORE: LATEST ODDS SINGLE LOOKUP ───────────────────

def best_odds_decimal_prob(event_type:str, event_label:str, participant:str, cutoff_dt:datetime, fut_conn):
    tbl = futures_table_map.get((event_type, event_label))
    if not tbl:
        return 1.0, 0.0
    alias = team_alias_map.get(participant, participant)
    with with_cursor(fut_conn) as cur:
        cur.execute(
            f"SELECT {','.join(sportsbook_cols)} FROM {tbl} WHERE team_name=%s AND date_created<=%s ORDER BY date_created DESC LIMIT 1",
            (alias, cutoff_dt))
        row = cur.fetchone()
    if not row:
        return 1.0, 0.0
    nums = [cast_odds(row[c]) for c in sportsbook_cols if cast_odds(row[c])]
    if not nums:
        return 1.0, 0.0
    best = max(nums)
    return american_odds_to_decimal(best), american_odds_to_prob(best)

# ─────────────────── EV TABLE PAGE (placeholder) ───────────────────

def ev_table_page():
    st.write("EV Table page – not modified in this script example.")

# ─────────────────── PROBABILITY PLOT PAGE ───────────────────

def probability_plot_page():
    st.subheader("Daily Implied Probability (Top‑5)")
    fut_conn = new_futures_conn()

    etype = st.selectbox("Event Type", sorted({t for (t, _) in futures_table_map}), key="pp_et")
    elabel = st.selectbox("Event Label", sorted({l for (t, l) in futures_table_map if t == etype}), key="pp_el")

    col1, col2 = st.columns(2)
    sd = col1.date_input("Start", datetime.utcnow().date() - timedelta(days=120))
    ed = col2.date_input("End", datetime.utcnow().date())
    if sd > ed:
        st.error("Start date must not be after End date"); return

    if not st.button("Plot", key="pp_plot"):
        return

    tbl = futures_table_map[(etype, elabel)]

    with with_cursor(fut_conn) as cur:
        cur.execute(f"SELECT team_name, date_created, {','.join(sportsbook_cols)} FROM {tbl} WHERE date_created BETWEEN %s AND %s", (f"{sd} 00:00:00", f"{ed} 23:59:59"))
        raw = pd.DataFrame(cur.fetchall())
    if raw.empty:
        st.warning("No odds data in that range."); return

    # compute best odds -> prob
    raw[sportsbook_cols] = raw[sportsbook_cols].apply(pd.to_numeric, errors='coerce').fillna(0)
    raw['best'] = raw[sportsbook_cols].replace(0, pd.NA).max(axis=1).fillna(0).astype(int)
    raw['prob'] = raw['best'].apply(american_odds_to_prob)
    raw['date'] = pd.to_datetime(raw['date_created']).dt.date

    # keep last snapshot per day per team
    raw = (raw.sort_values(['team_name','date'])
             .groupby(['team_name','date'])
             .tail(1)[['team_name','date','prob']])

    # pivot to daily grid for each team
    date_range = pd.date_range(sd, ed, freq='D')
    all_frames = []
    for name, grp in raw.groupby('team_name'):
        if grp.empty:
            continue
        g = grp.set_index('date').reindex(date_range).ffill()
        if 'prob' not in g.columns:
            continue
        g = g[['prob']].reset_index()
        g['team_name'] = name
        all_frames.append(g)

    daily = pd.concat(all_frames, ignore_index=True)

    # select top‑5 by probability on end date
    last_probs = daily[daily['date'] == daily['date'].max()].sort_values('prob', ascending=False)
    top5 = last_probs.head(5)['team_name'].tolist()
    daily_top = daily[daily['team_name'].isin(top5)]

    # plot
    fig, ax = plt.subplots(figsize=(11,6))
    for name, grp in daily_top.groupby('team_name'):
        ax.plot(grp['date'], grp['prob']*100, linewidth=2, label=name)
    ax.set_ylim(0, 100)
    ax.set_ylabel('Implied Probability (%)')
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
    ax.yaxis.set_major_formatter(PercentFormatter())
    ax.set_title(f"{elabel} – Daily Implied Probability (Top‑5)")
    ax.spines['top'].set_visible(False); ax.spines['right'].set_visible(False)
    plt.xticks(rotation=45)
    ax.legend(title='Name', bbox_to_anchor=(1.02,1), loc='upper left', frameon=False)
    st.pyplot(fig, use_container_width=True)

# ─────────────────── SIDEBAR NAV ───────────────────

page = st.sidebar.radio("Choose Page", ["EV Table", "Probability Plot"])

if page == "EV Table":
    ev_table_page()
else:
    probability_plot_page()
