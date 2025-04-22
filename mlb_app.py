import streamlit as st
import pymysql
from collections import defaultdict
import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import re

st.set_page_config(page_title="NBA Futures Dashboard", layout="wide")

# --- DB Connection Helpers ---
def get_betting_conn():
    return pymysql.connect(
        host=st.secrets["BETTING_DB"]["host"],
        user=st.secrets["BETTING_DB"]["user"],
        password=st.secrets["BETTING_DB"]["password"],
        database=st.secrets["BETTING_DB"]["database"],
        cursorclass=pymysql.cursors.DictCursor
    )

def get_futures_conn():
    return pymysql.connect(
        host=st.secrets["FUTURES_DB"]["host"],
        user=st.secrets["FUTURES_DB"]["user"],
        password=st.secrets["FUTURES_DB"]["password"],
        database=st.secrets["FUTURES_DB"]["database"],
        cursorclass=pymysql.cursors.DictCursor
    )

# --- Odds Helpers ---
def american_odds_to_decimal(odds):
    return 1.0 + (odds / 100.0) if odds > 0 else 1.0 + (100.0 / abs(odds)) if odds != 0 else 1.0

def american_odds_to_probability(odds):
    return 100.0 / (odds + 100.0) if odds > 0 else abs(odds) / (abs(odds) + 100.0) if odds != 0 else 0.0

def safe_cast_odds(val):
    try:
        if isinstance(val, (int, float)): return int(val)
        if isinstance(val, str):
            m = re.search(r"[-+]?\d+", val)
            return int(m.group()) if m else 0
        return 0
    except: return 0

# --- Mappings ---
futures_table_map = {
    ("Championship", "NBA Championship"): "NBAChampionship",
    ("Conference Winner", "Eastern Conference"): "NBAEasternConference",
    ("Conference Winner", "Western Conference"): "NBAWesternConference",
    ("Most Valuable Player Award", "Award"): "NBAMVP"
}

team_alias_map = {"Philadelphia 76ers": "76ers", "Milwaukee Bucks": "Bucks", "Boston Celtics": "Celtics", "Denver Nuggets": "Nuggets"}  # add more if needed
sportsbook_cols = ["BetMGM", "DraftKings", "Caesars", "ESPNBet", "FanDuel", "BallyBet", "RiversCasino", "Bet365"]

def get_best_odds(event_type, event_label, participant, snapshot_date, conn):
    table = futures_table_map.get((event_type, event_label))
    if not table:
        return 0
    alias = team_alias_map.get(participant, participant)
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT {','.join(sportsbook_cols)} FROM {table}
            WHERE team_name = %s AND date_created <= %s
            ORDER BY date_created DESC LIMIT 1
        """, (alias, snapshot_date))
        row = cur.fetchone()
    if not row:
        return 0
    odds = [safe_cast_odds(row[col]) for col in sportsbook_cols]
    nonzero = [o for o in odds if o != 0]
    return max(nonzero) if nonzero else 0

# --- Return Plot Page ---
def render_return_plot():
    st.header("NBA Futures Return Plot")
    betting_conn = get_betting_conn()
    futures_conn = get_futures_conn()

    event_type = st.selectbox("Select Event Type", sorted(set(k[0] for k in futures_table_map)))
    options = [label for (etype, label) in futures_table_map if etype == event_type]
    event_label = st.selectbox("Select Event Label", options)
    start = st.date_input("Start Date", datetime.now().date() - timedelta(days=30))
    end = st.date_input("End Date", datetime.now().date())

    if st.button("Generate Return Plot"):
        cur = betting_conn.cursor()
        cur.execute(f"""
            SELECT b.WagerID, b.PotentialPayout, b.DollarsAtStake, b.DateTimePlaced, b.LegCount,
                   l.LegID, l.ParticipantName, l.EventType, l.EventLabel
            FROM bets b JOIN legs l ON b.WagerID = l.WagerID
            WHERE b.WhichBankroll = 'GreenAleph' AND b.WLCA = 'Active' AND l.LeagueName = 'NBA'
        """)
        bets = cur.fetchall()
        cur.close()

        bet_map = defaultdict(lambda: {"pot": 0.0, "stake": 0.0, "placed": None, "legs": []})
        for row in bets:
            w = row["WagerID"]
            bet_map[w]["pot"] = row["PotentialPayout"]
            bet_map[w]["stake"] = row["DollarsAtStake"]
            bet_map[w]["placed"] = row["DateTimePlaced"]
            bet_map[w]["legs"].append((row["EventType"], row["EventLabel"], row["ParticipantName"]))

        series = []
        for cur_date in pd.date_range(start=start, end=end):
            dt = datetime.combine(cur_date, datetime.max.time())
            total_net, total_stake = 0.0, 0.0
            for bet in bet_map.values():
                if bet["placed"] > dt: continue
                prob = 1.0
                decs = []
                for et, el, pn in bet["legs"]:
                    odds = get_best_odds(et, el, pn, dt, futures_conn)
                    if odds == 0: prob = 0.0; break
                    dec = american_odds_to_decimal(odds)
                    decs.append((dec, et))
                    prob *= american_odds_to_probability(odds)
                if prob == 0: continue
                net = (bet["pot"] * prob) - bet["stake"]
                sum_excess = sum(d - 1 for d, _ in decs)
                if sum_excess <= 0: continue
                for d, et in decs:
                    if et == event_type:
                        frac = (d - 1) / sum_excess
                        total_net += frac * net
                        total_stake += frac * bet["stake"]
            pct = (total_net / total_stake * 100) if total_stake > 0 else 0.0
            series.append((cur_date, pct))

        if series:
            dates, values = zip(*series)
            fig, ax = plt.subplots(figsize=(10, 5))
            ax.plot(dates, values, marker='o')
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
            ax.set_title(f"% Return Over Time: {event_type} - {event_label}")
            ax.set_ylabel("Return (%)")
            ax.set_xlabel("Date")
            plt.xticks(rotation=45)
            st.pyplot(fig)

# --- Routing ---
page = st.sidebar.radio("Select Page", ["EV Table", "Return Plot"])

if page == "EV Table":
    main()
elif page == "Return Plot":
    render_return_plot()
