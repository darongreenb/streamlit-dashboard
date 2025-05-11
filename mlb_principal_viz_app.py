import streamlit as st
import pandas as pd
import re
from collections import defaultdict
from datetime import datetime
import traceback

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
    df = pd.DataFrame(sample)
    # metrics
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("💸 Active Stake", f"${df['ActiveDollarsAtStake'].sum():,.2f}")
    col2.metric("📈 Expected Payout", f"${df['ActiveExpectedPayout'].sum():,.2f}")
    col3.metric("💰 Realized Net Profit", f"${df['RealizedNetProfit'].sum():,.2f}")
    col4.metric("⚡️ Expected Value", f"${df['ExpectedValue'].sum():,.2f}")
    st.markdown("### Market-Level Breakdown")
    st.dataframe(df, use_container_width=True)

# ─────────────────  DB HELPERS  ─────────────────
try:
    import pymysql
except ImportError:
    st.error("PyMySQL is not installed. Run `pip install pymysql`")
    st.stop()

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
        st.error(f"Betting-db connection failed: {e}")
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
        st.error(f"Futures-db connection failed: {e}")
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

# ─────────────────  ODDS HELPERS  ─────────────────
def american_odds_to_decimal(o):
    return 1 + o/100 if o>0 else 1 + 100/abs(o) if o<0 else 1

def american_odds_to_prob(o):
    return 100/(o+100) if o>0 else abs(o)/(abs(o)+100) if o<0 else 0

def cast_odds(v):
    if v in (None,"",0): return 0
    if isinstance(v,(int,float)): return int(v)
    m = re.search(r"[-+]?\d+", str(v))
    return int(m.group()) if m else 0

# ─────────────────  MAPPINGS  ─────────────────
futures_table_map = {
    ("Championship","NBA Championship"): "NBAChampionship",
    ("Conference Winner","Eastern Conference"): "NBAEasternConference",
    ("Conference Winner","Western Conference"): "NBAWesternConference",
    ("Defensive Player of Year Award","Award"): "NBADefensivePotY",
    ("Division Winner","Atlantic Division"): "NBAAtlantic",
    ("Division Winner","Central Division"): "NBACentral",
    ("Division Winner","Northwest Division"): "NBANorthwest",
    ("Division Winner","Pacific Division"): "NBAPacific",
    ("Division Winner","Southeast Division"): "NBASoutheast",
    ("Division Winner","Southwest Division"): "NBASouthwest",
    ("Most Improved Player Award","Award"): "NBAMIP",
    ("Most Valuable Player Award","Award"): "NBAMVP",
    ("Rookie of Year Award","Award"): "NBARotY",
    ("Sixth Man of Year Award","Award"): "NBASixthMotY",
}

team_alias_map = {
    "Philadelphia 76ers":"76ers","Milwaukee Bucks":"Bucks","Chicago Bulls":"Bulls","Cleveland Cavaliers":"Cavaliers",
    "Boston Celtics":"Celtics","Los Angeles Clippers":"Clippers","Memphis Grizzlies":"Grizzlies",
    "Atlanta Hawks":"Hawks","Miami Heat":"Heat","Charlotte Hornets":"Hornets","Utah Jazz":"Jazz",
    "Sacramento Kings":"Kings","New York Knicks":"Knicks","Los Angeles Lakers":"Lakers",
    "Orlando Magic":"Magic","Dallas Mavericks":"Mavericks","Brooklyn Nets":"Nets",
    "Denver Nuggets":"Nuggets","Indiana Pacers":"Pacers","New Orleans Pelicans":"Pelicans",
    "Detroit Pistons":"Pistons","Toronto Raptors":"Raptors","Houston Rockets":"Rockets",
    "San Antonio Spurs":"Spurs","Phoenix Suns":"Suns","Oklahoma City Thunder":"Thunder",
    "Minnesota Timberwolves":"Timberwolves","Portland Trail Blazers":"Trail Blazers",
    "Golden State Warriors":"Warriors","Washington Wizards":"Wizards",
}

sportsbook_cols = ["BetMGM","DraftKings","Caesars","ESPNBet","FanDuel","BallyBet","RiversCasino","Bet365"]

def best_odds_decimal_prob(event_type, event_label, participant, cutoff_dt, fut_conn, vig_map):
    tbl = futures_table_map.get((event_type,event_label))
    if not tbl or fut_conn is None:
        return 1.0, 0.0
    alias = team_alias_map.get(participant, participant)
    cur = with_cursor(fut_conn)
    if cur is None:
        return 1.0, 0.0
    cur.execute(
        f"SELECT {','.join(sportsbook_cols)} FROM {tbl}"
        " WHERE team_name=%s AND date_created<=%s"
        " ORDER BY date_created DESC LIMIT 100",
        (alias, cutoff_dt)
    )
    rows = cur.fetchall(); cur.close()
    for r in rows:
        odds = [cast_odds(r[c]) for c in sportsbook_cols if cast_odds(r[c])]
        if not odds:
            continue
        # pick “longest” = min implied prob
        best = min(odds, key=american_odds_to_prob)
        dec = american_odds_to_decimal(best)
        prob = american_odds_to_prob(best)*(1 - vig_map.get((event_type,event_label),0.05))
        return dec, prob
    return 1.0, 0.0

# ─────────────────  MAIN PAGE  ─────────────────
def ev_table_page():
    try:
        bet_conn = new_betting_conn()
        fut_conn = new_futures_conn()
        if bet_conn is None or fut_conn is None:
            display_demo_data(); return

        now = datetime.utcnow()
        vig_inputs = {k:0.05 for k in futures_table_map}

        cur = with_cursor(bet_conn)

        # Active bets
        cur.execute("""
          SELECT b.WagerID,b.PotentialPayout,b.DollarsAtStake,
                 l.EventType,l.EventLabel,l.ParticipantName
            FROM bets b JOIN legs l ON b.WagerID=l.WagerID
           WHERE b.WhichBankroll='GreenAleph'
             AND b.WLCA='Active'
             AND l.LeagueName='NBA'
        """)
        rows = cur.fetchall()
        active = defaultdict(lambda:{"pot":0,"stake":0,"legs":[]})
        for r in rows:
            w = active[r["WagerID"]]
            w["pot"] = w["pot"] or float(r["PotentialPayout"] or 0)
            w["stake"] = w["stake"] or float(r["DollarsAtStake"] or 0)
            w["legs"].append((r["EventType"], r["EventLabel"], r["ParticipantName"]))
        active_stake, active_exp = defaultdict(float), defaultdict(float)
        for data in active.values():
            pot, stake, legs = data["pot"], data["stake"], data["legs"]
            decs, prob = [], 1.0
            for et, el, pn in legs:
                dec, p = best_odds_decimal_prob(et, el, pn, now, fut_conn, vig_inputs)
                if p==0:
                    prob=0; break
                decs.append(dec); prob*=p
            if prob==0: continue
            exp_payout = pot*prob
            total_exc = sum(d-1 for d in decs)
            if total_exc<=0: continue
            for d in decs:
                w=(d-1)/total_exc
                active_stake[(et,el)] += w*stake
                active_exp[(et,el)]   += w*exp_payout

        # Settled bets
        cur.execute("""
          SELECT b.WagerID,b.NetProfit,
                 l.EventType,l.EventLabel,l.ParticipantName
            FROM bets b JOIN legs l ON b.WagerID=l.WagerID
           WHERE b.WhichBankroll='GreenAleph'
             AND b.WLCA IN ('Win','Loss','Cashout')
             AND l.LeagueName='NBA'
        """)
        rows = cur.fetchall()
        net_map, legs_map = defaultdict(float), defaultdict(list)
        for r in rows:
            net_map[r["WagerID"]] = float(r["NetProfit"] or 0)
            legs_map[r["WagerID"]].append((r["EventType"],r["EventLabel"],r["ParticipantName"]))
        realized = defaultdict(float)
        for wid, legs in legs_map.items():
            net = net_map[wid]
            decs = [best_odds_decimal_prob(et,el,pn,now,fut_conn, vig_inputs)[0] for et,el,pn in legs]
            total_exc = sum(d-1 for d in decs)
            if total_exc<=0: continue
            for d,(et,el,_) in zip(decs, legs):
                realized[(et,el)] += net*((d-1)/total_exc)

        cur.close(); bet_conn.close(); fut_conn.close()

        # build dataframe
        records = []
        for (et,el) in futures_table_map:
            records.append({
                "EventType": et,
                "EventLabel": el,
                "ActiveDollarsAtStake": round(active_stake[(et,el)],2),
                "ActiveExpectedPayout": round(active_exp[(et,el)],2),
                "RealizedNetProfit": round(realized[(et,el)],2),
                "ExpectedValue": round(active_exp[(et,el)] - active_stake[(et,el)] + realized[(et,el)],2)
            })
        df = pd.DataFrame(records).sort_values(["EventType","EventLabel"]).reset_index(drop=True)

        # summary metrics
        tot = df[["ActiveDollarsAtStake","ActiveExpectedPayout","RealizedNetProfit","ExpectedValue"]].sum()
        c1,c2,c3,c4 = st.columns(4)
        c1.metric("💸 Active Stake", f"${tot['ActiveDollarsAtStake']:,.2f}")
        c2.metric("📈 Expected Payout", f"${tot['ActiveExpectedPayout']:,.2f}")
        c3.metric("💰 Realized Net Profit", f"${tot['RealizedNetProfit']:,.2f}")
        c4.metric("⚡️ Expected Value", f"${tot['ExpectedValue']:,.2f}")

        # table
        st.markdown("### Market-Level Breakdown")
        st.dataframe(df.style.format({
            "ActiveDollarsAtStake":"${:,.2f}",
            "ActiveExpectedPayout":"${:,.2f}",
            "RealizedNetProfit":"${:,.2f}",
            "ExpectedValue":"${:,.2f}"
        }).applymap(
            lambda v: "color:green;font-weight:bold" if isinstance(v,(int,float)) and v>0 else
                      "color:red;font-weight:bold"   if isinstance(v,(int,float)) and v<0 else "",
            subset=["ExpectedValue"]
        ), use_container_width=True, height=700)

    except Exception as e:
        st.error(f"Unexpected error: {e}")
        st.code(traceback.format_exc())
        display_demo_data()

if __name__ == "__main__":
    ev_table_page()
