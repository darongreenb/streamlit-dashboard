# ─────────────────────  NBA Futures Dashboard: EV Table Page  ──────────────────────
import streamlit as st
import pandas as pd
import re
from collections import defaultdict
from datetime import datetime
import random
import traceback

# Import pymysql with error handling
try:
    import pymysql
except ImportError:
    st.error("PyMySQL is not installed. Please install it with `pip install pymysql`")
    st.stop()

# ─────────────────  PAGE CONFIG  ─────────────────
st.set_page_config(page_title="Futures EV Table", layout="wide")
st.markdown("<h1 style='text-align: center;'>Futures EV Table</h1>", unsafe_allow_html=True)
st.markdown("<h3 style='text-align: center; color: gray;'>NBA markets (active + settled) plus settled futures from other leagues</h3>", unsafe_allow_html=True)

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
        st.error(f"Bet DB connection error {e.args[0]}")
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
        st.error(f"Futures DB connection error {e.args[0]}")
        return None


def with_cursor(conn):
    if conn is None:
        return None
    try:
        conn.ping(reconnect=True)
        return conn.cursor()
    except Exception as e:
        st.error(f"Cursor error: {e}")
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

# ─────────────────  MAPS  ────────────────────────

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
    # … (same as before) …
}

sportsbook_cols = [
    "BetMGM",
    "DraftKings",
    "Caesars",
    "ESPNBet",
    "FanDuel",
    "BallyBet",
    "RiversCasino",
    "Bet365",
]

# ─────────────────  BEST‑ODDS LOOKUP  ────────────────

def best_odds_decimal_prob(event_type, event_label, participant, cutoff_dt, fut_conn, vig_map):
    if fut_conn is None:
        return 1.0, 0.0
    tbl = futures_table_map.get((event_type, event_label))
    if not tbl:
        return 1.0, 0.0
    alias = team_alias_map.get(participant, participant)
    cur = with_cursor(fut_conn)
    if cur is None:
        return 1.0, 0.0
    try:
        cur.execute(
            f"""SELECT date_created, {','.join(sportsbook_cols)}
                     FROM {tbl}
                    WHERE team_name=%s AND date_created<=%s
                 ORDER BY date_created DESC
                    LIMIT 100""",
            (alias, cutoff_dt),
        )
        rows = cur.fetchall()
    except Exception as e:
        st.error(f"Odds query error for {participant}: {e}")
        rows = []
    finally:
        cur.close()

    for r in rows:
        quotes = [cast_odds(r.get(c)) for c in sportsbook_cols]
        quotes = [q for q in quotes if q]
        if not quotes:
            continue
        best = min(quotes, key=american_odds_to_prob)
        dec = american_odds_to_decimal(best)
        prob = american_odds_to_prob(best)
        vig = vig_map.get((event_type, event_label), 0.05)
        return dec, prob * (1 - vig)
    return 1.0, 0.0

# ─────────────────  EV TABLE PAGE  ────────────────

def ev_table_page():
    try:
        bet_conn = new_betting_conn()
        fut_conn = new_futures_conn()
        if bet_conn is None or fut_conn is None:
            st.warning("⚠️ DB connection failed. Showing demo data.")
            display_demo_data()
            return
        now = datetime.utcnow()

        vig_inputs = {k: 0.05 for k in futures_table_map}

        # ---------------- NBA Active wagers ----------------
        sql_active = """
            SELECT b.WagerID, b.PotentialPayout, b.DollarsAtStake,
                   l.EventType, l.EventLabel, l.ParticipantName
              FROM bets b JOIN legs l ON b.WagerID=l.WagerID
             WHERE b.WhichBankroll='GreenAleph' AND b.WLCA='Active' AND l.LeagueName='NBA'"""
        cur = with_cursor(bet_conn)
        cur.execute(sql_active)
        rows = cur.fetchall()

        # Use (league,event_type,event_label) as key
        active_stake, active_exp = defaultdict(float), defaultdict(float)
        for r in rows:
            pot = float(r["PotentialPayout"] or 0)
            stake = float(r["DollarsAtStake"] or 0)
            legs_key = ("NBA", r["EventType"], r["EventLabel"])
            dec, prob = best_odds_decimal_prob(r["EventType"], r["EventLabel"], r["ParticipantName"], now, fut_conn, vig_inputs)
            if prob == 0:
                continue
            expected = pot * prob
            exc = dec - 1
            if exc <= 0:
                continue
            active_stake[legs_key] += stake
            active_exp[legs_key] += expected

        # ---------------- NBA Realized ----------------
        sql_real_nba = """
            SELECT b.NetProfit,
                   l.EventType, l.EventLabel
              FROM bets b JOIN legs l ON b.WagerID=l.WagerID
             WHERE b.WhichBankroll='GreenAleph'
               AND b.WLCA IN ('Win','Loss','Cashout')
               AND l.LeagueName='NBA'"""
        cur.execute(sql_real_nba)
        rows = cur.fetchall()
        realized_np = defaultdict(float)
        for r in rows:
            key = ("NBA", r["EventType"], r["EventLabel"])
            realized_np[key] += float(r["NetProfit"] or 0)

        # ---------------- Non‑NBA settled rows ----------------
        sql_other = """
            SELECT b.WagerID, b.NetProfit,
                   l.EventType, l.EventLabel, l.LeagueName
              FROM bets b JOIN legs l ON b.WagerID=l.WagerID
             WHERE b.WhichBankroll='GreenAleph'
               AND b.WLCA IN ('Win','Loss','Cashout')
               AND l.LeagueName <> 'NBA'"""
        cur.execute(sql_other)
        rows = cur.fetchall()
        parlay_legs = defaultdict(list)
        parlay_net = {}
        for r in rows:
            wid = r["WagerID"]
            parlay_legs[wid].append((r["LeagueName"], r["EventType"], r["EventLabel"]))
            parlay_net[wid] = float(r["NetProfit"] or 0)
        for wid, legs in parlay_legs.items():
            chosen = min(legs)  # deterministic: first alphabetically
            league, et, el = chosen
            key = (league, et, el)
            realized_np[key] += parlay_net[wid]

        cur.close()
        bet_conn.close()
        fut_conn.close()

        # ---------------- assemble dataframe ----------------
        keys = set(active_stake) | set(active_exp) | set(realized_np)
        rows_out = []
        for (lg, et, el) in sorted(keys):
            stake = active_stake.get((lg, et, el), 0)
            exp = active_exp.get((lg, et, el), 0)
            net = realized_np.get((lg, et, el), 0)
            rows_out.append(
                dict(
                    LeagueName=lg,
                    EventType=et,
                    EventLabel=el,
                    ActiveDollarsAtStake=round(stake, 2),
                    ActiveExpectedPayout=round(exp, 2),
                    RealizedNetProfit=round(net, 2),
                    ExpectedValue=round(exp - stake + net, 2),
                )
            )
        df = pd.DataFrame(rows_out).sort_values(["LeagueName", "EventType", "EventLabel"]).reset_index(drop=True)
        display_data(df)
    except Exception as e:
        st.error(f"Unexpected error: {e}")
        st.code(traceback.format_exc())
        display_demo_data()

# ─────────────────  DISPLAY HELPERS  ────────────────

def display_data(df):
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("💸 Active Stake", f"${df['ActiveDollarsAtStake'].sum():,.0f}")
    col2.metric("📈 Expected Payout", f"${df['ActiveExpectedPayout'].sum():,.0f}")
    col3.metric("💰 Realized Net Profit", f"${df['RealizedNetProfit'].sum():,.0f}")
    col4.metric("⚡️ Expected Value", f"${df['ExpectedValue'].sum():,.0f}")

    def highlight_ev(v):
        return "color: green; font-weight:bold" if v > 0 else "color:red; font-weight:bold" if v < 0 else ""

    st.dataframe(
        df.style.format(
            {
                "ActiveDollarsAtStake": "${:,.0f}",
                "ActiveExpectedPayout": "${:,.0f}",
                "RealizedNetProfit": "${:,.0f}",
                "ExpectedValue": "${:,.0f}",
            }
        ).applymap(highlight_ev, subset=["ExpectedValue"]),
        use_container_width=True,
        height=700,
    )


def display_demo_data():
    st.info("Demo data — no DB connection")
    sample = [
        dict(
            LeagueName="NBA",
            EventType="Championship",
            EventLabel="NBA Championship",
            ActiveDollarsAtStake=0,
            ActiveExpectedPayout=0,
            RealizedNetProfit=2500,
            ExpectedValue=2500,
        ),
        dict(
            LeagueName="MLB",
            EventType="World Series",
            EventLabel="MLB Championship",
            ActiveDollarsAtStake=0,
            ActiveExpectedPayout=0,
            RealizedNetProfit=1500,
            ExpectedValue=1500,
        ),
    ]
    display_data(pd.DataFrame(sample))

# ─────────────────  MAIN  ────────────────

if __name__ == "__main__":
    ev_table_page()
