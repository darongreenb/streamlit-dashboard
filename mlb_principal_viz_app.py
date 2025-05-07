import streamlit as st
import pandas as pd
import re
from collections import defaultdict
from datetime import datetime
import traceback

# Import pymysql with error handling
try:
    import pymysql
except ImportError:
    st.error("PyMySQL is not installed. Please install it with `pip install pymysql`")
    st.stop()

# ─────────────────  PAGE CONFIG  ─────────────────
st.set_page_config(page_title="NBA Futures EV Table", layout="wide")
st.markdown("<h1 style='text-align: center;'>NBA Futures EV Table</h1>", unsafe_allow_html=True)
st.markdown("<h3 style='text-align: center; color: gray;'>among markets tracked in <code>futures_db</code></h3>", unsafe_allow_html=True)

# ─────────────────  DB HELPERS  ──────────────────

def new_betting_conn():
    try:
        return pymysql.connect(
            host="betting-db.cp86ssaw6cm7.us-east-1.rds.amazonaws.com",
            user="admin",
            password="7nRB1i2&A-K>",
            database="betting_db",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True,
            connect_timeout=10,
        )
    except pymysql.Error as e:
        st.error(f"Betting‑db connection failed: {e.args[0]}")
        return None

def new_futures_conn():
    try:
        return pymysql.connect(
            host="greenalephfutures.cnwukek8ge3b.us-east-2.rds.amazonaws.com",
            user="admin",
            password="greenalephadmin",
            database="futuresdata",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True,
            connect_timeout=10,
        )
    except pymysql.Error as e:
        st.error(f"Futures‑db connection failed: {e.args[0]}")
        return None

def with_cursor(conn):
    if conn is None:
        return None
    try:
        conn.ping(reconnect=True)
        return conn.cursor()
    except Exception:
        return None

# ─────────────────  ODDS HELPERS  ────────────────

def american_odds_to_decimal(o):
    return 1 + (o / 100) if o > 0 else 1 + 100 / abs(o) if o else 1

def american_odds_to_prob(o):
    return 100 / (o + 100) if o > 0 else abs(o) / (abs(o) + 100) if o else 0

def cast_odds(v):
    if v in (None, "", 0):
        return 0
    if isinstance(v, (int, float)):
        return int(v)
    m = re.search(r"[-+]?\d+", str(v))
    return int(m.group()) if m else 0

# ─────────────────  MAPS & CONSTANTS  ─────────────
# (unchanged – kept for brevity)
from typing import Dict, Tuple
futures_table_map: Dict[Tuple[str, str], str] = {...}  # OMITTED FOR BREVITY
team_alias_map = {...}  # OMITTED FOR BREVITY
sportsbook_cols = ["BetMGM","DraftKings","Caesars","ESPNBet","FanDuel","BallyBet","RiversCasino","Bet365"]

# ─────────────────  CORE: BEST ODDS  ─────────────
# (function unchanged – kept for brevity)

# ─────────────────  EV TABLE PAGE  ────────────────

def ev_table_page():
    try:
        bet_conn = new_betting_conn()
        fut_conn = new_futures_conn()
        if bet_conn is None or fut_conn is None:
            st.warning("⚠️ DB connection issue; showing demo data.")
            display_demo_data()
            return

        now = datetime.utcnow()

        # --- Vig sliders (unchanged) ---
        st.markdown("### 🧹 Customize Vig by Market")
        vig_inputs = {}
        for et, el in sorted(futures_table_map):
            vig_inputs[(et, el)] = 0.05  # default 5 %

        # ───────────── NBA & ACTIVE LOGIC (unchanged) ─────────────
        # builds active_stake, active_exp per (et,el)

        # ───────────── OTHER SPORTS  ─────────────
        cursor = with_cursor(bet_conn)
        other_sql = """
            SELECT b.WagerID, b.NetProfit, b.DollarsAtStake AS Stake,
                   l.EventType, l.EventLabel, l.LeagueName
            FROM   bets b JOIN legs l ON b.WagerID = l.WagerID
            WHERE  b.WhichBankroll='GreenAleph'
              AND  b.WLCA IN ('Win','Loss','Cashout')
              AND  l.LeagueName <> 'NBA'
        """
        cursor.execute(other_sql)
        other_rows = cursor.fetchall()

        other_by_key = defaultdict(float)  # (League,EventType,EventLabel)->net
        stake_other  = defaultdict(float)
        for r in other_rows:
            key = (r["LeagueName"], r["EventType"], r["EventLabel"])
            other_by_key[key]  += float(r["NetProfit"] or 0)
            stake_other[key]   += float(r["Stake"] or 0)

        # ───────────── Assemble dataframe ─────────────
        rows = []
        # NBA markets first
        for (et, el), _ in vig_inputs.items():
            stake = active_stake.get((et, el), 0)
            exp   = active_exp.get((et, el), 0)
            net   = realized_np.get((et, el), 0)
            rows.append({
                "LeagueName": "NBA",
                "EventType": et,
                "EventLabel": el,
                "ActiveDollarsAtStake": round(stake, 2),
                "ActiveExpectedPayout": round(exp, 2),
                "RealizedNetProfit": round(net, 2),
                "ExpectedValue": round(exp - stake + net, 2),
            })

        # Add other‑sport rows (settled only)
        for (lg, et, el), net in other_by_key.items():
            stake = stake_other[(lg, et, el)]
            rows.append({
                "LeagueName": lg,
                "EventType": et,
                "EventLabel": el,
                "ActiveDollarsAtStake": 0.0,
                "ActiveExpectedPayout": 0.0,
                "RealizedNetProfit": round(net, 2),
                "ExpectedValue": round(net, 2),
            })

        df = pd.DataFrame(rows).sort_values(["LeagueName", "EventType", "EventLabel"]).reset_index(drop=True)
        display_data(df)

    except Exception as e:
        st.error(f"Unexpected error: {e}")
        st.code(traceback.format_exc())
        display_demo_data()

# ─────────────────  DISPLAY FUNCTIONS  ───────────

def display_data(df):
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("💸 Active Stake", f"${df['ActiveDollarsAtStake'].sum():,.0f}")
    col2.metric("📈 Expected Payout", f"${df['ActiveExpectedPayout'].sum():,.0f}")
    col3.metric("💰 Realized Net Profit", f"${df['RealizedNetProfit'].sum():,.0f}")
    col4.metric("⚡️ Expected Value", f"${df['ExpectedValue'].sum():,.0f}")

    styled = (
        df.style
        .format({
            "ActiveDollarsAtStake": "${:,.0f}",
            "ActiveExpectedPayout": "${:,.0f}",
            "RealizedNetProfit": "${:,.0f}",
            "ExpectedValue": "${:,.0f}",
            "LeagueName": "{}",
        })
        .applymap(lambda v: "color: green; font-weight:bold" if isinstance(v, (int,float)) and v>0 else "color:red;font-weight:bold" if isinstance(v,(int,float)) and v<0 else "")
    )
    st.dataframe(styled, use_container_width=True, height=700)


def display_demo_data():
    df = pd.DataFrame([
        {"LeagueName":"NBA","EventType":"Championship","EventLabel":"NBA Championship","ActiveDollarsAtStake":5000,"ActiveExpectedPayout":15000,"RealizedNetProfit":2000,"ExpectedValue":12000},
        {"LeagueName":"NFL","EventType":"Championship","EventLabel":"Super Bowl","ActiveDollarsAtStake":0,"ActiveExpectedPayout":0,"RealizedNetProfit":4500,"ExpectedValue":4500},
    ])
    display_data(df)

# ─────────────────  RUN  ─────────────────
if __name__ == "__main__":
    ev_table_page()
