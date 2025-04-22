import streamlit as st
import mysql.connector
from collections import defaultdict
import pandas as pd
import re

# ─── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(page_title="NBA Futures EV Dashboard", layout="wide")
st.title("NBA Futures: Active & Realized Payouts by Market")

# ─── 1) Database connections ───────────────────────────────────────────────────

@st.experimental_singleton
def get_betting_conn():
    try:
        return mysql.connector.connect(
            host=st.secrets["betting_db"]["host"],
            user=st.secrets["betting_db"]["user"],
            password=st.secrets["betting_db"]["password"],
            database=st.secrets["betting_db"]["database"],
        )
    except Exception as e:
        st.error(f"⚠️ Could not connect to betting_db: {e}")
        raise

@st.experimental_singleton
def get_futures_conn():
    try:
        return mysql.connector.connect(
            host=st.secrets["futures_db"]["host"],
            user=st.secrets["futures_db"]["user"],
            password=st.secrets["futures_db"]["password"],
            database=st.secrets["futures_db"]["database"],
        )
    except Exception as e:
        st.error(f"⚠️ Could not connect to futures_db: {e}")
        raise

betting_conn = get_betting_conn()
futures_conn = get_futures_conn()

# ─── 2) Odds conversion & helpers ───────────────────────────────────────────────

def american_odds_to_decimal(odds: int) -> float:
    if odds == 0:
        return 1.0
    return 1.0 + (odds / 100.0) if odds > 0 else 1.0 + (100.0 / abs(odds))

def american_odds_to_probability(odds: int) -> float:
    if odds == 0:
        return 0.0
    return 100.0 / (odds + 100.0) if odds > 0 else abs(odds) / (abs(odds) + 100.0)

def safe_cast_odds(val):
    try:
        if isinstance(val, (int, float)):
            return int(val)
        if isinstance(val, str):
            m = re.search(r"[-+]?\d+", val)
            return int(m.group()) if m else 0
        return 0
    except:
        return 0

# ─── 3) Table & team mappings ──────────────────────────────────────────────────

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

# ─── 4) Fetch best available odds ────────────────────────────────────────────────

def get_best_decimal_and_probability(event_type, event_label, participant):
    tbl = futures_table_map.get((event_type, event_label))
    if not tbl:
        st.debug(f"Missing mapping ({event_type},{event_label})")
        return 1.0, 0.0

    alias = team_alias_map.get(participant, participant)
    cur = futures_conn.cursor(dictionary=True)
    cur.execute(f"""
        SELECT date_created, {','.join(sportsbook_cols)}
          FROM {tbl}
         WHERE team_name = %s
         ORDER BY date_created DESC
    """, (alias,))
    rows = cur.fetchall()
    cur.close()

    for r in rows:
        odds = [safe_cast_odds(r[c]) for c in sportsbook_cols]
        nonzero = [o for o in odds if o!=0]
        if nonzero:
            best = max(nonzero)
            return american_odds_to_decimal(best), american_odds_to_probability(best)
    st.debug(f"No non‑zero odds for {alias} in {tbl}")
    return 1.0, 0.0

# ─── 5) Query Active bets ───────────────────────────────────────────────────────

active_cursor = betting_conn.cursor(dictionary=True)
active_cursor.execute("""
SELECT b.WagerID, b.PotentialPayout, b.DollarsAtStake, l.LegID,
       l.EventType, l.EventLabel, l.ParticipantName
  FROM bets b
  JOIN legs l ON b.WagerID=l.WagerID
 WHERE b.WhichBankroll='GreenAleph'
   AND l.LeagueName='NBA'
   AND b.WLCA='Active'
""")
active_rows = active_cursor.fetchall()
active_cursor.close()

active_bets = defaultdict(lambda: {"pot":0.0, "stake":0.0, "legs":[]})
for r in active_rows:
    w=r["WagerID"]
    ab=active_bets[w]
    ab["pot"]   = ab["pot"]   or float(r["PotentialPayout"] or 0)
    ab["stake"] = ab["stake"] or float(r["DollarsAtStake"] or 0)
    ab["legs"].append({
        "etype":r["EventType"], "elabel":r["EventLabel"], "part":r["ParticipantName"]
    })

active_stake = defaultdict(float)
active_payout = defaultdict(float)

for w,data in active_bets.items():
    pot, stake, legs = data["pot"], data["stake"], data["legs"]
    parlay_p=1.0; decs=[]
    for leg in legs:
        d,p = get_best_decimal_and_probability(leg["etype"], leg["elabel"], leg["part"])
        parlay_p *= p
        decs.append((d,leg["etype"],leg["elabel"]))
    if parlay_p==0: continue
    exp_pay = pot*parlay_p
    s_exc = sum(d-1 for d,_,_ in decs)
    if s_exc<=0: continue
    for d,et,el in decs:
        frac=(d-1)/s_exc
        active_stake[(et,el)] += frac*stake
        active_payout[(et,el)] += frac*exp_pay

# ─── 6) Query Realized bets ────────────────────────────────────────────────────

real_cursor = betting_conn.cursor(dictionary=True)
real_cursor.execute("""
SELECT b.WagerID, b.NetProfit, b.WLCA, l.EventType, l.EventLabel, l.ParticipantName
  FROM bets b
  JOIN legs l ON b.WagerID=l.WagerID
 WHERE b.WhichBankroll='GreenAleph'
   AND l.LeagueName='NBA'
   AND b.WLCA IN ('Win','Loss','Cashout')
""")
real_rows = real_cursor.fetchall()
real_cursor.close()
betting_conn.close()

real_bets = defaultdict(lambda: {"net":0.0, "wlca":None, "legs":[]})
for r in real_rows:
    w=r["WagerID"]; rb=real_bets[w]
    rb["wlca"]=rb["wlca"] or r["WLCA"]
    rb["net"] = rb["net"] or float(r["NetProfit"] or 0)
    rb["legs"].append({"etype":r["EventType"],"elabel":r["EventLabel"],"part":r["ParticipantName"]})

realized = defaultdict(float)
for w,data in real_bets.items():
    net,legs = data["net"],data["legs"]
    decs=[]
    for leg in legs:
        d,_ = get_best_decimal_and_probability(leg["etype"], leg["elabel"], leg["part"])
        decs.append((d,leg["etype"],leg["elabel"]))
    s_exc=sum(d-1 for d,_,_ in decs)
    if s_exc<=0: continue
    for d,et,el in decs:
        frac=(d-1)/s_exc
        realized[(et,el)] += frac*net

# ─── 7) Combine & display ─────────────────────────────────────────────────────

records = []
keys = set(active_stake)|set(active_payout)|set(realized)
for et,el in sorted(keys):
    stake = active_stake.get((et,el),0)
    pay   = active_payout.get((et,el),0)
    net   = realized.get((et,el),0)
    ev    = pay - stake + net
    records.append({
        "EventType": et,
        "EventLabel": el,
        "ActiveDollarsAtStake": round(stake,2),
        "ActiveExpectedPayout":   round(pay,2),
        "RealizedNetProfit":      round(net,2),
        "ExpectedValue":          round(ev,2)
    })

df = pd.DataFrame(records)
df = df.sort_values(["EventType","EventLabel"]).reset_index(drop=True)

# full‑width display
st.dataframe(df, use_container_width=True)
