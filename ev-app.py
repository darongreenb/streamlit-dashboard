import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from datetime import datetime, timedelta
import pymysql
import re
from collections import defaultdict
import traceback

# ────────────────────────────────────────────────────────────────────
# PAGE CONFIG
st.set_page_config(page_title="NBA Futures Historical EV", layout="wide")
st.markdown("<h1 style='text-align:center'>NBA Futures Historical EV</h1>", unsafe_allow_html=True)
st.markdown("<h3 style='text-align:center;color:gray'>Weekly EV Tracking</h3>", unsafe_allow_html=True)

# ────────────────────────────────────────────────────────────────────
# DB HELPERS (same as EV Table page)
def new_betting_conn():
    try:
        return pymysql.connect(
            host=st.secrets['BETTING_DB']['host'],
            user=st.secrets['BETTING_DB']['user'],
            password=st.secrets['BETTING_DB']['password'],
            database=st.secrets['BETTING_DB']['database'],
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True,
            connect_timeout=10
        )
    except pymysql.Error as e:
        st.error(f"Betting DB conn error {e.args[0]}")
        return None


def new_futures_conn():
    try:
        return pymysql.connect(
            host=st.secrets['FUTURES_DB']['host'],
            user=st.secrets['FUTURES_DB']['user'],
            password=st.secrets['FUTURES_DB']['password'],
            database=st.secrets['FUTURES_DB']['database'],
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True,
            connect_timeout=10
        )
    except pymysql.Error as e:
        st.error(f"Futures DB conn error {e.args[0]}")
        return None


def with_cursor(conn):
    if conn is None:
        return None
    try:
        conn.ping(reconnect=True)
        return conn.cursor()
    except Exception:
        return None

# ────────────────────────────────────────────────────────────────────
# ODDS HELPERS & MAPS (same as EV Table page)
def american_odds_to_decimal(o):
    return 1.0 + (o/100) if o > 0 else 1.0 + 100/abs(o) if o else 1.0

def american_odds_to_prob(o):
    return 100/(o+100) if o > 0 else abs(o)/(abs(o)+100) if o else 0.0


def cast_odds(v):
    if v in (None, '', 0):
        return 0
    if isinstance(v, (int, float)):
        return int(v)
    m = re.search(r"[-+]?\d+", str(v))
    return int(m.group()) if m else 0

futures_table_map = {
    ('Championship','NBA Championship'): 'NBAChampionship',
    ('Conference Winner','Eastern Conference'): 'NBAEasternConference',
    ('Conference Winner','Western Conference'): 'NBAWesternConference',
    ('Defensive Player of Year Award','Award'): 'NBADefensivePotY',
    ('Division Winner','Atlantic Division'): 'NBAAtlantic',
    ('Division Winner','Central Division'): 'NBACentral',
    ('Division Winner','Northwest Division'): 'NBANorthwest',
    ('Division Winner','Pacific Division'): 'NBAPacific',
    ('Division Winner','Southeast Division'): 'NBASoutheast',
    ('Division Winner','Southwest Division'): 'NBASouthwest',
    ('Most Improved Player Award','Award'): 'NBAMIP',
    ('Most Valuable Player Award','Award'): 'NBAMVP',
    ('Rookie of Year Award','Award'): 'NBARotY',
    ('Sixth Man of Year Award','Award'): 'NBASixthMotY',
}

team_alias_map = {
    'Philadelphia 76ers':'76ers','Milwaukee Bucks':'Bucks','Chicago Bulls':'Bulls',
    'Cleveland Cavaliers':'Cavaliers','Boston Celtics':'Celtics','Los Angeles Clippers':'Clippers',
    'Memphis Grizzlies':'Grizzlies','Atlanta Hawks':'Hawks','Miami Heat':'Heat',
    'Charlotte Hornets':'Hornets','Utah Jazz':'Jazz','Sacramento Kings':'Kings',
    'New York Knicks':'Knicks','Los Angeles Lakers':'Lakers','Orlando Magic':'Magic',
    'Dallas Mavericks':'Mavericks','Brooklyn Nets':'Nets','Denver Nuggets':'Nuggets',
    'Indiana Pacers':'Pacers','New Orleans Pelicans':'Pelicans','Detroit Pistons':'Pistons',
    'Toronto Raptors':'Raptors','Houston Rockets':'Rockets','San Antonio Spurs':'Spurs',
    'Phoenix Suns':'Suns','Oklahoma City Thunder':'Thunder','Minnesota Timberwolves':'Timberwolves',
    'Portland Trail Blazers':'Trail Blazers','Golden State Warriors':'Warriors','Washington Wizards':'Wizards',
}

sportsbook_cols = [
    'BetMGM','DraftKings','Caesars','ESPNBet','FanDuel','BallyBet','RiversCasino','Bet365'
]

# ────────────────────────────────────────────────────────────────────
def best_odds_decimal_prob(event_type, event_label, participant, cutoff_dt, fut_conn, vig_map):
    tbl = futures_table_map.get((event_type, event_label))
    if not tbl or fut_conn is None:
        return 1.0, 0.0
    alias = team_alias_map.get(participant, participant)
    cur = with_cursor(fut_conn)
    if cur is None:
        return 1.0, 0.0
    cur.execute(
        f"SELECT {','.join(sportsbook_cols)} FROM {tbl} WHERE team_name=%s AND date_created<=%s ORDER BY date_created DESC LIMIT 1",
        (alias, cutoff_dt)
    )
    row = cur.fetchone()
    if not row:
        return 1.0, 0.0
    nums = [cast_odds(row[c]) for c in sportsbook_cols if row[c]]
    if not nums:
        return 1.0, 0.0
    best = max(nums)
    dec = american_odds_to_decimal(best)
    prob = american_odds_to_prob(best)
    vig = vig_map.get((event_type, event_label), 0.05)
    return dec, prob * (1 - vig)

# ────────────────────────────────────────────────────────────────────
# SQL QUERIES (identical to EV Table page)
sql_active = """
SELECT b.WagerID, b.PotentialPayout, b.DollarsAtStake, l.EventType, l.EventLabel, l.ParticipantName
FROM bets b JOIN legs l ON b.WagerID = l.WagerID
WHERE b.WhichBankroll='GreenAleph' AND b.WLCA='Active' AND l.LeagueName='NBA'
"""

sql_real = """
SELECT b.WagerID, b.NetProfit, l.EventType, l.EventLabel, l.ParticipantName
FROM bets b JOIN legs l ON b.WagerID = l.WagerID
WHERE b.WhichBankroll='GreenAleph' AND b.WLCA IN ('Win','Loss','Cashout') AND l.LeagueName='NBA'
"""

# ────────────────────────────────────────────────────────────────────
def historical_ev_page():
    bet_conn = new_betting_conn()
    fut_conn = new_futures_conn()
    if not bet_conn or not fut_conn:
        st.warning("Database connection failed. Showing demo data.")
        return

    # Date range
    start = st.sidebar.date_input("Start", datetime.utcnow().date() - timedelta(days=180))
    end = st.sidebar.date_input("End", datetime.utcnow().date())
    if start > end:
        st.error("Start date must be before end date")
        return

    # Vig settings
    default_vig = st.sidebar.slider("Default Vig %", 0, 20, 5)
    vig_map = {}
    for et, el in futures_table_map:
        vig_map[(et, el)] = st.sidebar.slider(f"{et} – {el}", 0, 20, default_vig) / 100.0

    # Weekly dates
    dates = [start + timedelta(days=7 * i) for i in range(((end - start).days // 7) + 1)]
    ev_records = []

    for dt in dates:
        snapshot = datetime.combine(dt, datetime.min.time())

        # Active
        cur = with_cursor(bet_conn)
        cur.execute(sql_active)
        rows = cur.fetchall()
        active_bets = defaultdict(lambda: {"pot": 0, "stake": 0, "legs": []})
        for r in rows:
            wid = r["WagerID"]
            active_bets[wid]["pot"] = float(r["PotentialPayout"] or 0)
            active_bets[wid]["stake"] = float(r["DollarsAtStake"] or 0)
            active_bets[wid]["legs"].append((r["EventType"], r["EventLabel"], r["ParticipantName"]))

        active_exp = 0.0
        active_stake = 0.0
        for data in active_bets.values():
            pot = data["pot"]; stake = data["stake"]; legs = data["legs"]
            prob = 1.0; decs = []
            for et, el, pn in legs:
                dec, p = best_odds_decimal_prob(et, el, pn, snapshot, fut_conn, vig_map)
                prob *= p; decs.append(dec)
            if prob > 0 and decs:
                active_stake += stake
                active_exp += pot * prob

        # Realized
        cur.execute(sql_real)
        rows = cur.fetchall()
        realized = 0.0
        for r in rows:
            net = float(r["NetProfit"] or 0)
            # allocate across legs
            decs = []
            for et, el, pn in [(r["EventType"], r["EventLabel"], r["ParticipantName"])]:
                dec, _ = best_odds_decimal_prob(et, el, pn, snapshot, fut_conn, vig_map)
                decs.append(dec)
            if decs:
                tot = sum(d - 1 for d in decs)
                for d in decs:
                    realized += net * ((d - 1) / tot) if tot else 0

        total_ev = active_exp - active_stake + realized
        ev_records.append({"Date": snapshot, "Total EV": total_ev})

    df = pd.DataFrame(ev_records)
    fig = go.Figure(
        go.Scatter(x=df["Date"], y=df["Total EV"], mode="lines+markers", name="Total EV", line=dict(color="green"))
    )
    fig.update_layout(title="Weekly NBA Futures EV", template="plotly_white")
    st.plotly_chart(fig, use_container_width=True)

if __name__ == '__main__':
    historical_ev_page()
