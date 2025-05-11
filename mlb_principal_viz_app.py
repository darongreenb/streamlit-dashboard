# ─────────────────────  NBA Futures Dashboard: EV Table Page  ──────────────────────
import streamlit as st
import pandas as pd
import re
from collections import defaultdict
from datetime import datetime
import traceback
from typing import Dict, Tuple

# Ensure PyMySQL is present
try:
    import pymysql
except ImportError:
    st.error("PyMySQL is not installed. Run:  pip install pymysql")
    st.stop()

# ─────────────────────  PAGE CONFIG  ─────────────────────
st.set_page_config(page_title="Futures EV Table", layout="wide")
st.markdown("<h1 style='text-align:center'>NBA Futures EV Table</h1>", unsafe_allow_html=True)
st.markdown("<h3 style='text-align:center;color:gray'>among markets tracked in <code>futures_db</code></h3> plus settled non-NBA bets", unsafe_allow_html=True)

# ─────────────────────  DB HELPERS  ─────────────────────
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

# ─────────────────────  ODDS HELPERS  ─────────────────────
def american_odds_to_decimal(o):  # 100 → 2.0, ‑110 → 1.91
    return 1 + (o / 100) if o > 0 else 1 + 100 / abs(o) if o else 1


def american_odds_to_prob(o):     # 100 → 0.50, ‑110 → 0.524
    return 100 / (o + 100) if o > 0 else abs(o) / (abs(o) + 100) if o else 0


def cast_odds(v):
    if v in (None, "", 0):
        return 0
    if isinstance(v, (int, float)):
        return int(v)
    m = re.search(r"[-+]?\d+", str(v))
    return int(m.group()) if m else 0

# ─────────────────────  MAPS & CONSTANTS  ─────────────────────
futures_table_map: Dict[Tuple[str, str], str] = {
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

team_alias_map: Dict[str, str] = {
    "Philadelphia 76ers": "76ers", "Milwaukee Bucks": "Bucks", "Chicago Bulls": "Bulls",
    "Cleveland Cavaliers": "Cavaliers", "Boston Celtics": "Celtics", "Los Angeles Clippers": "Clippers",
    "Memphis Grizzlies": "Grizzlies", "Atlanta Hawks": "Hawks", "Miami Heat": "Heat",
    "Charlotte Hornets": "Hornets", "Utah Jazz": "Jazz", "Sacramento Kings": "Kings",
    "New York Knicks": "Knicks", "Los Angeles Lakers": "Lakers", "Orlando Magic": "Magic",
    "Dallas Mavericks": "Mavericks", "Brooklyn Nets": "Nets", "Denver Nuggets": "Nuggets",
    "Indiana Pacers": "Pacers", "New Orleans Pelicans": "Pelicans", "Detroit Pistons": "Pistons",
    "Toronto Raptors": "Raptors", "Houston Rockets": "Rockets", "San Antonio Spurs": "Spurs",
    "Phoenix Suns": "Suns", "Oklahoma City Thunder": "Thunder", "Minnesota Timberwolves": "Timberwolves",
    "Portland Trail Blazers": "Trail Blazers", "Golden State Warriors": "Warriors", "Washington Wizards": "Wizards",
}

sportsbook_cols = [
    "BetMGM", "DraftKings", "Caesars", "ESPNBet", "FanDuel",
    "BallyBet", "RiversCasino", "Bet365",
]

# ─────────────────────  BEST‑ODDS (unchanged)  ─────────────────────
def best_odds_decimal_prob(event_type, event_label, participant,
                           cutoff_dt, fut_conn, vig_map):
    if fut_conn is None:
        return 1.0, 0.0
    tbl = futures_table_map.get((event_type, event_label))
    if not tbl:
        return 1.0, 0.0
    alias = team_alias_map.get(participant, participant)
    cur = with_cursor(fut_conn)
    if cur is None:
        return 1.0, 0.0
    cur.execute(
        f"""SELECT {','.join(sportsbook_cols)}
              FROM {tbl}
             WHERE team_name=%s AND date_created<=%s
         ORDER BY date_created DESC LIMIT 100""",
        (alias, cutoff_dt),
    )
    rows = cur.fetchall()
    cur.close()

    for r in rows:
        quotes = [cast_odds(r.get(c)) for c in sportsbook_cols if cast_odds(r.get(c))]
        if not quotes:
            continue
        best = min(quotes, key=american_odds_to_prob)  # least probable (“longest”)
        dec = american_odds_to_decimal(best)
        prob = american_odds_to_prob(best) * (1 - vig_map.get((event_type, event_label), 0.05))
        return dec, prob
    return 1.0, 0.0

# ─────────────────────  EV TABLE PAGE  ─────────────────────
def ev_table_page():
    try:
        bet_conn = new_betting_conn()
        fut_conn = new_futures_conn()
        if bet_conn is None or fut_conn is None:
            st.warning("⚠️ DB connection issue; showing demo data.")
            display_demo_data(); return

        now = datetime.utcnow()
        vig_inputs = {k: 0.05 for k in futures_table_map}   # flat 5 %

        # ---------- ACTIVE NBA FUTURES ----------
        cursor = with_cursor(bet_conn)
        cursor.execute("""
            SELECT b.WagerID, b.PotentialPayout, b.DollarsAtStake,
                   l.EventType, l.EventLabel, l.ParticipantName
              FROM bets b JOIN legs l ON b.WagerID=l.WagerID
             WHERE b.WhichBankroll='GreenAleph'
               AND b.WLCA='Active'
               AND l.LeagueName='NBA'
        """)
        rows = cursor.fetchall()

        active_bets = defaultdict(lambda: {"pot":0,"stake":0,"legs":[]})
        for r in rows:
            w = active_bets[r["WagerID"]]
            w["pot"] = w["pot"] or float(r["PotentialPayout"] or 0)
            w["stake"] = w["stake"] or float(r["DollarsAtStake"] or 0)
            w["legs"].append((r["EventType"], r["EventLabel"], r["ParticipantName"]))

        active_stake, active_exp = defaultdict(float), defaultdict(float)
        for data in active_bets.values():
            pot, stake, legs = data.values()
            decs, prob = [], 1.0
            for et, el, pn in legs:
                dec, p = best_odds_decimal_prob(et, el, pn, now, fut_conn, vig_inputs)
                if p == 0:
                    prob = 0; break
                decs.append(dec); prob *= p
            if prob == 0:
                continue
            expected = pot * prob
            exc_sum = sum(d - 1 for d in decs)
            if exc_sum <= 0:
                continue
            for d in decs:
                w = (d - 1) / exc_sum
                active_stake[(et, el)] += w * stake
                active_exp[(et, el)] += w * expected

        # ---------- REALIZED NBA FUTURES ----------
        cursor.execute("""
            SELECT b.WagerID, b.NetProfit,
                   l.EventType, l.EventLabel, l.ParticipantName
              FROM bets b JOIN legs l ON b.WagerID=l.WagerID
             WHERE b.WhichBankroll='GreenAleph'
               AND b.WLCA IN ('Win','Loss','Cashout')
               AND l.LeagueName='NBA'
        """)
        rows = cursor.fetchall()

        wager_net, wager_legs = defaultdict(float), defaultdict(list)
        for r in rows:
            wager_net[r["WagerID"]] = float(r["NetProfit"] or 0)
            wager_legs[r["WagerID"]].append((r["EventType"], r["EventLabel"], r["ParticipantName"]))

        realized_np = defaultdict(float)
        for wid, legs in wager_legs.items():
            net = wager_net[wid]
            decs = [best_odds_decimal_prob(et, el, pn, now, fut_conn, vig_inputs)[0] for et, el, pn in legs]
            exc_sum = sum(d - 1 for d in decs)
            if exc_sum <= 0:
                continue
            for d, (et, el, _) in zip(decs, legs):
                realized_np[(et, el)] += net * ((d - 1) / exc_sum)

        # ---------- COMPLETED OTHER SPORTS ----------
        cursor.execute("""
            SELECT b.NetProfit, b.DollarsAtStake AS Stake,
                   l.EventType, l.EventLabel, l.LeagueName
              FROM bets b JOIN legs l ON b.WagerID=l.WagerID
             WHERE b.WhichBankroll='GreenAleph'
               AND b.WLCA IN ('Win','Loss','Cashout')
               AND l.LeagueName <> 'NBA'
        """)
        rows = cursor.fetchall()
        other_by_key = defaultdict(float)
        stake_other  = defaultdict(float)
        for r in rows:
            key = (r["LeagueName"], r["EventType"], r["EventLabel"])
            other_by_key[key] += float(r["NetProfit"] or 0)
            stake_other[key]  += float(r["Stake"] or 0)

        # ---------- BUILD DATAFRAME ----------
        records = []
        # NBA rows
        for (et, el) in futures_table_map:
            stake = active_stake[(et, el)]
            exp   = active_exp[(et, el)]
            net   = realized_np[(et, el)]
            records.append(dict(
                LeagueName="NBA", EventType=et, EventLabel=el,
                ActiveDollarsAtStake=round(stake,2),
                ActiveExpectedPayout=round(exp,2),
                RealizedNetProfit=round(net,2),
                ExpectedValue=round(exp - stake + net, 2),
            ))
        # Other‑sport rows
        for (lg, et, el), net in other_by_key.items():
            records.append(dict(
                LeagueName=lg, EventType=et, EventLabel=el,
                ActiveDollarsAtStake=0.0,
                ActiveExpectedPayout=0.0,
                RealizedNetProfit=round(net,2),
                ExpectedValue=round(net,2),
            ))

        df = pd.DataFrame(records).sort_values(
            ["LeagueName", "EventType", "EventLabel"]
        ).reset_index(drop=True)

        display_data(df)

    except Exception as e:
        st.error(f"Unexpected error: {e}")
        st.code(traceback.format_exc())
        display_demo_data()

# ─────────────────────  DISPLAY HELPERS  ─────────────────────
def display_data(df):
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("💸 Active Stake", f"${df['ActiveDollarsAtStake'].sum():,.0f}")
    col2.metric("📈 Expected Payout", f"${df['ActiveExpectedPayout'].sum():,.0f}")
    col3.metric("💰 Realized Net Profit", f"${df['RealizedNetProfit'].sum():,.0f}")
    col4.metric("⚡️ Expected Value", f"${df['ExpectedValue'].sum():,.0f}")

    style = (
        df.style.format({
            "ActiveDollarsAtStake": "${:,.0f}",
            "ActiveExpectedPayout": "${:,.0f}",
            "RealizedNetProfit": "${:,.0f}",
            "ExpectedValue": "${:,.0f}",
        })
        .applymap(
            lambda v: "color:green;font-weight:bold"
            if isinstance(v, (int, float)) and v > 0
            else "color:red;font-weight:bold"
            if isinstance(v, (int, float)) and v < 0
            else ""
        )
    )
    st.dataframe(style, use_container_width=True, height=720)

def display_demo_data():
    demo = pd.DataFrame([
        dict(LeagueName="NBA", EventType="Championship", EventLabel="NBA Championship",
             ActiveDollarsAtStake=5000, ActiveExpectedPayout=15000,
             RealizedNetProfit=2000, ExpectedValue=12000),
        dict(LeagueName="NFL", EventType="Championship", EventLabel="Super Bowl",
             ActiveDollarsAtStake=0, ActiveExpectedPayout=0,
             RealizedNetProfit=4500, ExpectedValue=4500),
    ])
    display_data(demo)

# ─────────────────────  RUN  ─────────────────────
if __name__ == "__main__":
    ev_table_page()
