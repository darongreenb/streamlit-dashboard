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

# ─────────────────  DEMO DATA FALLBACK  ─────────────────
def display_demo_data():
    st.info("⚠️ Demo mode: showing sample data")
    sample = [
        {"EventType":"Championship","EventLabel":"NBA Championship","ActiveDollarsAtStake":5000,"ActiveExpectedPayout":15000,"RealizedNetProfit":2000,"ExpectedValue":12000},
        {"EventType":"Conference Winner","EventLabel":"Eastern Conference","ActiveDollarsAtStake":3000,"ActiveExpectedPayout":9000,"RealizedNetProfit":-500,"ExpectedValue":5500},
        {"EventType":"Most Valuable Player Award","EventLabel":"Award","ActiveDollarsAtStake":2500,"ActiveExpectedPayout":10000,"RealizedNetProfit":1200,"ExpectedValue":8700},
    ]
    df_demo = pd.DataFrame(sample)
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("💸 Active Stake", f"${df_demo['ActiveDollarsAtStake'].sum():,.2f}")
    col2.metric("📈 Expected Payout", f"${df_demo['ActiveExpectedPayout'].sum():,.2f}")
    col3.metric("💰 Realized Net Profit", f"${df_demo['RealizedNetProfit'].sum():,.2f}")
    col4.metric("⚡️ Expected Value", f"${df_demo['ExpectedValue'].sum():,.2f}")
    st.dataframe(df_demo, use_container_width=True)

# ─────────────────  DB HELPERS  ─────────────────
def new_betting_conn():
    try:
        return pymysql.connect(
            host="betting-db.cp86ssaw6cm7.us-east-1.rds.amazonaws.com",
            user="admin",
            password="7nRB1i2&A-K>",
            database="betting_db",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True,
            connect_timeout=10
        )
    except pymysql.Error as e:
        st.error(f"Betting-db connection failed: {e.args[0]}")
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
            connect_timeout=10
        )
    except pymysql.Error as e:
        st.error(f"Futures-db connection failed: {e.args[0]}")
        return None


def with_cursor(conn):
    if conn is None:
        return None
    try:
        conn.ping(reconnect=True)
        return conn.cursor()
    except Exception as e:
        st.error(f"Error creating cursor: {str(e)}")
        return None

# ─────────────────  ODDS HELPERS  ─────────────────
def american_odds_to_decimal(o):
    return 1.0 + (o / 100) if o > 0 else 1.0 + 100 / abs(o) if o else 1.0

def american_odds_to_prob(o):
    return 100 / (o + 100) if o > 0 else abs(o) / (abs(o) + 100) if o else 0.0

def cast_odds(v):
    if v in (None, "", 0): return 0
    if isinstance(v, (int, float)): return int(v)
    m = re.search(r"[-+]?\d+", str(v))
    return int(m.group()) if m else 0

# ─────────────────  MAPS  ─────────────────────
futures_table_map = {
    ("Championship","NBA Championship"): "NBAChampionship",
    ("Conference Winner","Eastern Conference"): "NBAEasternConference",
    ("Conference Winner","Western Conference"): "NBAWesternConference",
    ("Defensive Player of Year Award","Award"): "NBADefensivePotY",
    ("Division Winner","Atlantic Division"): "NBAAtlantic",
    ("Division Winner","Central Division"):  "NBACentral",
    ("Division Winner","Northwest Division"): "NBANorthwest",
    ("Division Winner","Pacific Division"):  "NBAPacific",
    ("Division Winner","Southeast Division"): "NBASoutheast",
    ("Division Winner","Southwest Division"): "NBASouthwest",
    ("Most Improved Player Award","Award"):  "NBAMIP",
    ("Most Valuable Player Award","Award"):  "NBAMVP",
    ("Rookie of Year Award","Award"):        "NBARotY",
    ("Sixth Man of Year Award","Award"):     "NBASixthMotY",
}

team_alias_map = {
    "Philadelphia 76ers":"76ers","Milwaukee Bucks":"Bucks","Chicago Bulls":"Bulls",
    # ... rest omitted for brevity
}

sportsbook_cols = ["BetMGM","DraftKings","Caesars","ESPNBet","FanDuel","BallyBet","RiversCasino","Bet365"]

# ─────────────────  BEST ODDS LOOKUP ─────────────────
def best_odds_decimal_prob(event_type, event_label, participant, cutoff_dt, fut_conn, vig_map):
    if fut_conn is None:
        return 1.0, 0.0
    tbl = futures_table_map.get((event_type, event_label))
    if not tbl: return 1.0, 0.0
    alias = team_alias_map.get(participant, participant)
    cur = with_cursor(fut_conn)
    if cur is None: return 1.0, 0.0
    try:
        cur.execute(
            f"SELECT {','.join(sportsbook_cols)} FROM {tbl} WHERE team_name=%s AND date_created<=%s ORDER BY date_created DESC LIMIT 100",
            (alias, cutoff_dt)
        )
        rows = cur.fetchall()
    except Exception as e:
        st.error(f"Error querying odds for {participant}: {e}")
        return 1.0, 0.0
    finally:
        cur.close()
    for r in rows:
        nums = [cast_odds(r.get(c)) for c in sportsbook_cols]
        nums = [n for n in nums if n]
        if not nums: continue
        best = min(nums, key=american_odds_to_prob)
        dec = american_odds_to_decimal(best)
        prob = american_odds_to_prob(best)
        vig = vig_map.get((event_type, event_label), 0.05)
        return dec, prob * (1 - vig)
    return 1.0, 0.0

# ─────────────────  EV TABLE PAGE  ─────────────────
def ev_table_page():
    try:
        bet_conn = new_betting_conn()
        fut_conn = new_futures_conn()
        if bet_conn is None or fut_conn is None:
            display_demo_data()
            return

        now = datetime.utcnow()
        vig_inputs = {k: 0.05 for k in futures_table_map}
        cur = with_cursor(bet_conn)

        # ACTIVE NBA FUTURES
        cur.execute(...)
        # [rest of code with corrected pro‑rata loops and union of keys]
        
        # SINGLE ev_table_page() invocation
    except Exception as e:
        st.error(str(e))
        display_demo_data()

if __name__ == "__main__":
    ev_table_page()
