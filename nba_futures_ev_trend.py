import streamlit as st
import pandas as pd
import pymysql
import re
from collections import defaultdict
from datetime import datetime, date

# ────────────────────────────────────────────────────────────────────
# PAGE CONFIG
# ────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="NBA Futures EV & History", layout="wide")
st.title("🧮 NBA Futures Expected Value & History")

# ────────────────────────────────────────────────────────────────────
# DATABASE HELPERS
# ────────────────────────────────────────────────────────────────────
def new_betting_conn():
    return pymysql.connect(
        host="betting-db.cp86ssaw6cm7.us-east-1.rds.amazonaws.com",
        user="admin",
        password="7nRB1i2&A-K>",
        database="betting_db",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )

def new_futures_conn():
    return pymysql.connect(
        host="greenalephfutures.cnwukek8ge3b.us-east-2.rds.amazonaws.com",
        user="admin",
        password="greenalephadmin",
        database="futuresdata",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )

def with_cursor(conn):
    conn.ping(reconnect=True)
    return conn.cursor()

# ────────────────────────────────────────────────────────────────────
# ODDS CONVERSION
# ────────────────────────────────────────────────────────────────────
def american_odds_to_decimal(o):
    return 1 + (o/100) if o>0 else 1 + 100/abs(o) if o<0 else 1

def american_odds_to_prob(o):
    return 100/(o+100) if o>0 else abs(o)/(abs(o)+100) if o<0 else 0

def cast_odds(v):
    if v in (None, "", 0): return 0
    if isinstance(v, (int, float)): return int(v)
    m = re.search(r"[-+]?\d+", str(v))
    return int(m.group()) if m else 0

# ────────────────────────────────────────────────────────────────────
# FUTURES MARKETS MAPPING
# ────────────────────────────────────────────────────────────────────
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
team_alias_map = { }
# (populate alias map as before...)
sportsbook_cols = ["BetMGM","DraftKings","Caesars","ESPNBet","FanDuel","BallyBet","RiversCasino","Bet365"]

# ────────────────────────────────────────────────────────────────────
# FETCH BEST ODDS
# ────────────────────────────────────────────────────────────────────
def best_odds_decimal_prob(event_type, label, participant, cutoff, fut_conn, vig=0.05):
    tbl = futures_table_map.get((event_type, label))
    if not tbl: return 1.0, 0.0
    alias = participant
    cur = with_cursor(fut_conn)
    cur.execute(
        f"SELECT {','.join(sportsbook_cols)} FROM {tbl} WHERE team_name=%s AND date_created<=%s ORDER BY date_created DESC LIMIT 100",
        (alias, cutoff)
    )
    for row in cur.fetchall():
        odds = [cast_odds(row[c]) for c in sportsbook_cols if cast_odds(row[c])]
        if not odds: continue
        # pick longest: min implied prob
        best = min(odds, key=american_odds_to_prob)
        dec = american_odds_to_decimal(best)
        prob = american_odds_to_prob(best)*(1-vig)
        cur.close()
        return dec, prob
    cur.close()
    return 1.0, 0.0

# ────────────────────────────────────────────────────────────────────
# BUILD CURRENT EV
# ────────────────────────────────────────────────────────────────────
def compute_current_ev():
    bet_conn = new_betting_conn()
    fut_conn = new_futures_conn()
    now = datetime.utcnow()
    vig=0.05
    cur = with_cursor(bet_conn)
    # active
    sql_act = """
    SELECT b.WagerID,b.PotentialPayout,b.DollarsAtStake,l.EventType,l.EventLabel,l.ParticipantName
    FROM bets b JOIN legs l ON b.WagerID=l.WagerID
    WHERE b.WhichBankroll='GreenAleph' AND b.WLCA='Active' AND l.LeagueName='NBA'"""
    cur.execute(sql_act)
    act_rows=cur.fetchall()
    active=defaultdict(lambda:{'pot':0,'stake':0,'legs':[]})
    for r in act_rows:
        w=active[r['WagerID']]
        w['pot']=w['pot'] or float(r['PotentialPayout'] or 0)
        w['stake']=w['stake'] or float(r['DollarsAtStake'] or 0)
        w['legs'].append((r['EventType'],r['EventLabel'],r['ParticipantName']))
    total_stake=0; total_payout=0; total_real=0
    # calc active EV
    for w in active.values():
        pot,stk=w['pot'],w['stake']; legs=w['legs']
        decs=[]; prob=1
        for et,el,pn in legs:
            dec,p=best_odds_decimal_prob(et,el,pn,now,fut_conn,vig)
            if p==0: prob=0; break
            decs.append(dec); prob*=p
        if prob>0:
            total_stake+=stk
            total_payout+=pot*prob
    # realized all leagues
    sql_real="""
    SELECT b.NetProfit FROM bets b WHERE b.WhichBankroll='GreenAleph' AND b.WLCA IN ('Win','Loss','Cashout')"""
    cur.execute(sql_real)
    for r in cur.fetchall(): total_real+=float(r['NetProfit'] or 0)
    bet_conn.close(); fut_conn.close()
    total_ev = total_payout - total_stake + total_real
    return total_ev

# ────────────────────────────────────────────────────────────────────
# SNAPSHOT & HISTORY
# ────────────────────────────────────────────────────────────────────

def save_snapshot(ev):
    conn = new_betting_conn()
    cur = with_cursor(conn)
    cur.execute(
        "REPLACE INTO ev_history (snapshot_date, expected_value) VALUES (%s,%s)",
        (date.today(), ev)
    )
    conn.close()

@st.cache_data
ndef load_history():
    conn=new_betting_conn()
    df=pd.read_sql("SELECT snapshot_date AS date, expected_value AS ev FROM ev_history ORDER BY snapshot_date", conn, parse_dates=['date'])
    conn.close()
    return df

# ────────────────────────────────────────────────────────────────────
# STREAMLIT LAYOUT
# ────────────────────────────────────────────────────────────────────

# 1) Show current EV and a button to save it
st.subheader("Current Total Expected Value")
today_ev = compute_current_ev()
st.metric("Total EV", f"${today_ev:,.2f}")
if st.button("➕ Add Today’s EV to History"):
    save_snapshot(today_ev)
    st.success("Snapshot saved 📅✅")

# 2) Load history and plot
hist = load_history()
st.subheader("Historical EV Over Time")
st.line_chart(hist.rename(columns={'date':'index'}).set_index('date')['ev'])

# 3) Show raw history
with st.expander("View EV History Table"):
    st.dataframe(hist.style.format({
        'date':'{:%Y-%m-%d}', 'ev':'${:,.2f}'
    }), use_container_width=True)
