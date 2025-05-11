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
    "Cleveland Cavaliers":"Cavaliers","Boston Celtics":"Celtics","Los Angeles Clippers":"Clippers",
    "Memphis Grizzlies":"Grizzlies","Atlanta Hawks":"Hawks","Miami Heat":"Heat",
    "Charlotte Hornets":"Hornets","Utah Jazz":"Jazz","Sacramento Kings":"Kings",
    "New York Knicks":"Knicks","Los Angeles Lakers":"Lakers","Orlando Magic":"Magic",
    "Dallas Mavericks":"Mavericks","Brooklyn Nets":"Nets","Denver Nuggets":"Nuggets",
    "Indiana Pacers":"Pacers","New Orleans Pelicans":"Pelicans","Detroit Pistons":"Pistons",
    "Toronto Raptors":"Raptors","Houston Rockets":"Rockets","San Antonio Spurs":"Spurs",
    "Phoenix Suns":"Suns","Oklahoma City Thunder":"Thunder","Minnesota Timberwolves":"Timberwolves",
    "Portland Trail Blazers":"Trail Blazers","Golden State Warriors":"Warriors","Washington Wizards":"Wizards",
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
            f"""SELECT {','.join(sportsbook_cols)}
                  FROM {tbl}
                 WHERE team_name=%s AND date_created<=%s
              ORDER BY date_created DESC LIMIT 100""",
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
        # pick longest (lowest implied prob)
        best = min(nums, key=american_odds_to_prob)
        dec = american_odds_to_decimal(best)
        prob = american_odds_to_prob(best)
        vig = vig_map.get((event_type, event_label), 0.05)
        return dec, prob * (1 - vig)
    return 1.0, 0.0

# ─────────────────  EV TABLE PAGE ─────────────────
def ev_table_page():
    try:
        bet_conn = new_betting_conn()
        fut_conn = new_futures_conn()
        if bet_conn is None or fut_conn is None:
            st.warning("⚠️ Unable to connect to one or more databases. Showing demo.")
            display_demo_data()
            return

        now = datetime.utcnow()
        vig_inputs = {k: 0.05 for k in futures_table_map}

        # Active NBA futures
        cursor = with_cursor(bet_conn)
        cursor.execute(
            """
            SELECT b.WagerID, b.PotentialPayout, b.DollarsAtStake,
                   l.EventType, l.EventLabel, l.ParticipantName
              FROM bets b JOIN legs l ON b.WagerID = l.WagerID
             WHERE b.WhichBankroll='GreenAleph'
               AND b.WLCA='Active'
               AND l.LeagueName='NBA'"""
        )
        active_rows = cursor.fetchall()
        active_bets = defaultdict(lambda: {"pot":0, "stake":0, "legs":[]})
        for r in active_rows:
            w = active_bets[r["WagerID"]]
            w["pot"] = w["pot"] or float(r["PotentialPayout"] or 0)
            w["stake"] = w["stake"] or float(r["DollarsAtStake"] or 0)
            w["legs"].append((r["EventType"], r["EventLabel"], r["ParticipantName"]))

        active_stake, active_exp = defaultdict(float), defaultdict(float)
        for data in active_bets.values():
            pot, stake, legs = data["pot"], data["stake"], data["legs"]
            decs, prob = [], 1.0
            for et, el, pn in legs:
                dec, p = best_odds_decimal_prob(et, el, pn, now, fut_conn, vig_inputs)
                if p == 0:
                    prob = 0
                    break
                decs.append(dec)
                prob *= p
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

        # Realized NBA futures
        cursor.execute(
            """
            SELECT b.WagerID, b.NetProfit,
                   l.EventType, l.EventLabel, l.ParticipantName
              FROM bets b JOIN legs l ON b.WagerID = l.WagerID
             WHERE b.WhichBankroll='GreenAleph'
               AND b.WLCA IN ('Win','Loss','Cashout')
               AND l.LeagueName='NBA'"""
        )
        real_rows = cursor.fetchall()
        wager_net, wager_legs = defaultdict(float), defaultdict(list)
        for r in real_rows:
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

        # ---------- Assemble table ----------
        rows_out = []
        for et, el in sorted(active_stake.keys() | active_exp.keys() | realized_np.keys()):
            stake = active_stake.get((et, el), 0)
            exp = active_exp.get((et, el), 0)
            net = realized_np.get((et, el), 0)
            rows_out.append({
                "EventType": et,
                "EventLabel": el,
                "ActiveDollarsAtStake": round(stake, 2),
                "ActiveExpectedPayout": round(exp, 2),
                "RealizedNetProfit": round(net, 2),
                "ExpectedValue": round(exp - stake + net, 2),
            })
        df = pd.DataFrame(rows_out)
        if not df.empty:
            df = df.sort_values(["EventType", "EventLabel"]).reset_index(drop=True)
            # Add TOTAL row
            totals = df[["ActiveDollarsAtStake", "ActiveExpectedPayout", "RealizedNetProfit", "ExpectedValue"]].sum()
            total_row = {
                "EventType": "TOTAL",
                "EventLabel": "",
                "ActiveDollarsAtStake": totals["ActiveDollarsAtStake"],
                "ActiveExpectedPayout": totals["ActiveExpectedPayout"],
                "RealizedNetProfit": totals["RealizedNetProfit"],
                "ExpectedValue": totals["ExpectedValue"],
            }
            df = pd.concat([df, pd.DataFrame([total_row])], ignore_index=True)

            styled = df.style.format("${:,.2f}", subset=[
                "ActiveDollarsAtStake",
                "ActiveExpectedPayout",
                "RealizedNetProfit",
                "ExpectedValue",
            ]).applymap(
                lambda v: "color: green; font-weight: bold" if isinstance(v, (int, float)) and v > 0 else
                          "color: red; font-weight: bold" if isinstance(v, (int, float)) and v < 0 else
                          "",
                subset=["ExpectedValue"],
            )

            st.markdown("### Market-Level Breakdown")
            st.dataframe(styled, use_container_width=True, height=700)
        else:
            st.info("No data available to display.")

        cursor.close(); bet_conn.close(); fut_conn.close()

    except Exception as e:
        st.error(f"An unexpected error occurred: {str(e)}")
        st.code(traceback.format_exc())
        display_demo_data()

# Demo fallback
def display_demo_data():
    data = [
        {"EventType":"Championship","EventLabel":"NBA Championship",
         "ActiveDollarsAtStake":5000,"ActiveExpectedPayout":15000,
         "RealizedNetProfit":2000,"ExpectedValue":12000},
    ]
    df = pd.DataFrame(data)
    totals = df[["ActiveDollarsAtStake","ActiveExpectedPayout","RealizedNetProfit","ExpectedValue"]].sum()
    total_row = {"EventType":"TOTAL","EventLabel":"",
                 "ActiveDollarsAtStake":totals["ActiveDollarsAtStake"],
                 "ActiveExpectedPayout":totals["ActiveExpectedPayout"],
                 "RealizedNetProfit":totals["RealizedNetProfit"],
                 "ExpectedValue":totals["ExpectedValue"]}
    df = pd.concat([df, pd.DataFrame([total_row])], ignore_index=True)
    styled = df.style.format("${:,.0f}").applymap(
        lambda v: "color: green; font-weight: bold" if isinstance(v, (int, float)) and v > 0 else "",
    )
    st.dataframe(styled, use_container_width=True, height=300)

# Run
if __name__ == "__main__":
    ev_table_page()
