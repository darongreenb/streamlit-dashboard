import streamlit as st
import pymysql
from collections import defaultdict
import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import re

# --- Page config ---
st.set_page_config(page_title="NBA Futures Dashboard", layout="wide")

# --- DB connections ---
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

# --- Odds helpers ---
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
    ("Most Valuable Player Award", "Award"): "NBAMVP",
}

team_alias_map = {
    "Philadelphia 76ers": "76ers", "Milwaukee Bucks": "Bucks", "Boston Celtics": "Celtics", "Denver Nuggets": "Nuggets"
}

sportsbook_cols = ["BetMGM", "DraftKings", "Caesars", "ESPNBet", "FanDuel", "BallyBet", "RiversCasino", "Bet365"]

def get_best_decimal_and_probability(event_type, event_label, participant, futures_conn):
    table = futures_table_map.get((event_type, event_label))
    if not table:
        return 1.0, 0.0
    alias = team_alias_map.get(participant, participant)
    with futures_conn.cursor() as cur:
        cur.execute(f"""
            SELECT {','.join(sportsbook_cols)} FROM {table}
            WHERE team_name = %s
            ORDER BY date_created DESC LIMIT 1
        """, (alias,))
        row = cur.fetchone()
    if row:
        odds = [safe_cast_odds(row[col]) for col in sportsbook_cols]
        nonzero = [o for o in odds if o != 0]
        if nonzero:
            best = max(nonzero)
            return american_odds_to_decimal(best), american_odds_to_probability(best)
    return 1.0, 0.0

# --- EV Table ---
def render_ev_table():
    st.title("NBA Futures: Active & Realized Payouts by Market")
    with get_betting_conn() as betting_conn, get_futures_conn() as futures_conn:
        active_stake, active_payout, realized_net = defaultdict(float), defaultdict(float), defaultdict(float)
        active_bets = defaultdict(lambda: {"pot": 0.0, "stake": 0.0, "legs": []})

        with betting_conn.cursor() as cur:
            cur.execute("""
                SELECT b.WagerID, b.PotentialPayout, b.DollarsAtStake,
                       l.EventType, l.EventLabel, l.ParticipantName
                FROM bets b JOIN legs l ON b.WagerID = l.WagerID
                WHERE b.WhichBankroll = 'GreenAleph' AND b.WLCA = 'Active' AND l.LeagueName = 'NBA'
            """)
            for row in cur.fetchall():
                bet = active_bets[row["WagerID"]]
                bet["pot"] = bet["pot"] or float(row["PotentialPayout"] or 0.0)
                bet["stake"] = bet["stake"] or float(row["DollarsAtStake"] or 0.0)
                bet["legs"].append((row["EventType"], row["EventLabel"], row["ParticipantName"]))

        for b in active_bets.values():
            pot, stake, legs = b["pot"], b["stake"], b["legs"]
            probs, decs = [], []
            for et, el, pn in legs:
                d, p = get_best_decimal_and_probability(et, el, pn, futures_conn)
                probs.append(p)
                decs.append((d, et, el))
            if 0 in probs:
                continue
            prob = 1.0
            for p in probs: prob *= p
            expected = pot * prob
            sum_excess = sum(d - 1.0 for d, _, _ in decs)
            if sum_excess <= 0: continue
            for d, et, el in decs:
                frac = (d - 1.0) / sum_excess
                active_stake[(et, el)] += frac * stake
                active_payout[(et, el)] += frac * expected

        with betting_conn.cursor() as cur:
            cur.execute("""
                SELECT b.WagerID, b.NetProfit, l.EventType, l.EventLabel, l.ParticipantName
                FROM bets b JOIN legs l ON b.WagerID = l.WagerID
                WHERE b.WhichBankroll = 'GreenAleph' AND b.WLCA IN ('Win','Loss','Cashout') AND l.LeagueName = 'NBA'
            """)
            results = cur.fetchall()

        realized_legs = defaultdict(list)
        for r in results:
            realized_legs[r["WagerID"]].append((r["EventType"], r["EventLabel"], r["ParticipantName"], float(r["NetProfit"] or 0.0)))

        for legs in realized_legs.values():
            net = legs[0][3]
            decs = [(get_best_decimal_and_probability(et, el, pn, futures_conn)[0], et, el) for et, el, pn, _ in legs]
            s_exc = sum(d - 1.0 for d, _, _ in decs)
            if s_exc <= 0: continue
            for d, et, el in decs:
                realized_net[(et, el)] += net * ((d - 1.0) / s_exc)

        records = []
        all_keys = set(active_stake) | set(active_payout) | set(realized_net)
        for k in sorted(all_keys):
            stake = active_stake.get(k, 0.0)
            payout = active_payout.get(k, 0.0)
            net = realized_net.get(k, 0.0)
            ev = payout - stake + net
            records.append({
                "EventType": k[0],
                "EventLabel": k[1],
                "ActiveDollarsAtStake": round(stake, 2),
                "ActiveExpectedPayout": round(payout, 2),
                "RealizedNetProfit": round(net, 2),
                "ExpectedValue": round(ev, 2),
            })

        df = pd.DataFrame(records).sort_values(["EventType", "EventLabel"]).reset_index(drop=True)
        st.dataframe(df, use_container_width=True)

# --- Return Plot ---
def render_return_plot():
    st.title("NBA Futures: % Return Plot")
    event_type = st.selectbox("Event Type", sorted(set(t[0] for t in futures_table_map)))
    event_label = st.selectbox("Event Label", sorted(set(t[1] for t in futures_table_map if t[0] == event_type)))
    start = st.date_input("Start Date", datetime.now().date() - timedelta(days=30))
    end = st.date_input("End Date", datetime.now().date())

    if start > end:
        st.error("Start date must be before end date")
        return

    if st.button("Generate Plot"):
        betting_conn = get_betting_conn()
        futures_conn = get_futures_conn()

        cur = betting_conn.cursor()
        cur.execute("""
            SELECT b.WagerID, b.PotentialPayout, b.DollarsAtStake, b.DateTimePlaced,
                   l.EventType, l.EventLabel, l.ParticipantName
            FROM bets b JOIN legs l ON b.WagerID = l.WagerID
            WHERE b.WhichBankroll = 'GreenAleph' AND b.WLCA = 'Active' AND l.LeagueName = 'NBA'
        """)
        rows = cur.fetchall()
        cur.close()

        bets = defaultdict(lambda: {"pot": 0.0, "stake": 0.0, "placed": None, "legs": []})
        for r in rows:
            b = bets[r["WagerID"]]
            b["pot"] = r["PotentialPayout"]
            b["stake"] = r["DollarsAtStake"]
            b["placed"] = r["DateTimePlaced"]
            b["legs"].append((r["EventType"], r["EventLabel"], r["ParticipantName"]))

        dates, values = [], []
        for dt in pd.date_range(start, end):
            dt_end = datetime.combine(dt, datetime.max.time())
            total_net, total_stake = 0.0, 0.0
            for b in bets.values():
                if b["placed"] > dt_end: continue
                ppot, stake, legs = b["pot"], b["stake"], b["legs"]
                prob, decs = 1.0, []
                for et, el, pn in legs:
                    odds = get_best_decimal_and_probability(et, el, pn, futures_conn)[0]
                    decs.append((odds, et))
                    prob *= american_odds_to_probability(odds)
                if prob == 0: continue
                net = (ppot * prob) - stake
                sum_excess = sum(d - 1.0 for d, _ in decs)
                if sum_excess <= 0: continue
                for d, et in decs:
                    if et == event_type:
                        frac = (d - 1.0) / sum_excess
                        total_net += frac * net
                        total_stake += frac * stake
            pct = (total_net / total_stake) * 100 if total_stake > 0 else 0
            dates.append(dt)
            values.append(pct)

        fig, ax = plt.subplots(figsize=(10, 5))
        ax.plot(dates, values, marker='o')
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
        ax.set_title(f"% Return Over Time for {event_type}")
        ax.set_ylabel("Return (%)")
        ax.set_xlabel("Date")
        plt.xticks(rotation=45)
        st.pyplot(fig)

# --- Page Router ---
page = st.sidebar.radio("Select Page", ["EV Table", "% Return Plot"])

if page == "EV Table":
    render_ev_table()
elif page == "% Return Plot":
    render_return_plot()
