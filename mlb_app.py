import streamlit as st
import mysql.connector
import pandas as pd
import re
from collections import defaultdict

###############################################################################
# 1) Streamlit Config & Title
###############################################################################
st.set_page_config(layout="wide")  # Make the layout wide
st.title("NBA Futures Analysis: Active vs. Realized Performance")

###############################################################################
# 2) Database Connections
###############################################################################
@st.cache_resource
def get_db_connections():
    # Connect to the betting_db
    conn_bets = mysql.connector.connect(
        host="betting-db.cp86ssaw6cm7.us-east-1.rds.amazonaws.com",
        user="admin",
        password="7nRB1i2&A-K>",
        database="betting_db"
    )
    # Connect to futuresdata
    conn_futures = mysql.connector.connect(
        host="greenalephfutures.cnwukek8ge3b.us-east-2.rds.amazonaws.com",
        user="admin",
        password="greenalephadmin",
        database="futuresdata"
    )
    return conn_bets, conn_futures

betting_conn, futures_conn = get_db_connections()

###############################################################################
# 3) Odds Conversion & Helpers
###############################################################################
def american_odds_to_decimal(odds: int) -> float:
    if odds == 0:
        return 1.0
    return 1.0 + (odds / 100.0) if odds > 0 else 1.0 + (100.0 / abs(odds))

def american_odds_to_probability(odds: int) -> float:
    if odds == 0:
        return 0.0
    return 100.0 / (odds + 100.0) if odds > 0 else float(abs(odds)) / (abs(odds) + 100.0)

def safe_cast_odds(val):
    try:
        if isinstance(val, int):
            return val
        elif isinstance(val, float):
            return int(val)
        elif isinstance(val, str):
            val = val.strip()
            if val.lower() in ("", "na", "null", "none"):
                return 0
            match = re.search(r"[-+]?\d+", val)
            if match:
                return int(match.group())
        return 0
    except Exception:
        return 0

###############################################################################
# 4) Table & Team Mappings
###############################################################################
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
    "Philadelphia 76ers": "76ers",
    "Milwaukee Bucks": "Bucks",
    "Chicago Bulls": "Bulls",
    "Cleveland Cavaliers": "Cavaliers",
    "Boston Celtics": "Celtics",
    "Los Angeles Clippers": "Clippers",
    "Memphis Grizzlies": "Grizzlies",
    "Atlanta Hawks": "Hawks",
    "Miami Heat": "Heat",
    "Charlotte Hornets": "Hornets",
    "Utah Jazz": "Jazz",
    "Sacramento Kings": "Kings",
    "New York Knicks": "Knicks",
    "Los Angeles Lakers": "Lakers",
    "Orlando Magic": "Magic",
    "Dallas Mavericks": "Mavericks",
    "Brooklyn Nets": "Nets",
    "Denver Nuggets": "Nuggets",
    "Indiana Pacers": "Pacers",
    "New Orleans Pelicans": "Pelicans",
    "Detroit Pistons": "Pistons",
    "Toronto Raptors": "Raptors",
    "Houston Rockets": "Rockets",
    "San Antonio Spurs": "Spurs",
    "Phoenix Suns": "Suns",
    "Oklahoma City Thunder": "Thunder",
    "Minnesota Timberwolves": "Timberwolves",
    "Portland Trail Blazers": "Trail Blazers",
    "Golden State Warriors": "Warriors",
    "Washington Wizards": "Wizards",
}
sportsbook_cols = ["BetMGM","DraftKings","Caesars","ESPNBet","FanDuel","BallyBet","RiversCasino","Bet365"]

###############################################################################
# 5) Best available odds from non-zero fields
###############################################################################
def get_best_decimal_and_probability(event_type, event_label, participant_name):
    table = futures_table_map.get((event_type, event_label))
    if not table:
        st.write(f"[DEBUG] Missing table mapping for ({event_type}, {event_label})")
        return 1.0, 0.0

    alias = team_alias_map.get(participant_name, participant_name)
    try:
        c = futures_conn.cursor(dictionary=True)
        c.execute(f"""
            SELECT date_created, {','.join(sportsbook_cols)}
            FROM {table}
            WHERE team_name = %s
            ORDER BY date_created DESC
        """, (alias,))
        all_rows = c.fetchall()
        c.close()

        for row in all_rows:
            numeric_odds = [safe_cast_odds(row[col]) for col in sportsbook_cols]
            numeric_odds = [o for o in numeric_odds if o != 0]
            if numeric_odds:
                best = max(numeric_odds)
                return (american_odds_to_decimal(best),
                        american_odds_to_probability(best))
        st.write(f"[DEBUG] No non-zero odds found for {alias} in {table}")
        return 1.0, 0.0
    except Exception as e:
        st.write(f"[ERROR] Odds fetch for {alias} in {table}: {e}")
        return 1.0, 0.0

###############################################################################
# 6) Query #1: Active => DollarsAtStake, ExpectedPayout
###############################################################################
active_stake_dict = defaultdict(float)
active_exp_dict   = defaultdict(float)

cur_active = betting_conn.cursor(dictionary=True)
cur_active.execute("""
SELECT
    b.WagerID, b.PotentialPayout, b.DollarsAtStake, b.WLCA, b.LegCount,
    l.LegID, l.ParticipantName, l.EventType, l.EventLabel
FROM bets b
JOIN legs l ON b.WagerID = l.WagerID
WHERE b.WhichBankroll='GreenAleph'
  AND l.LeagueName='NBA'
  AND b.WLCA='Active'
""")
rows_active = cur_active.fetchall()
cur_active.close()

active_bets = defaultdict(lambda: {
    "PotentialPayout": 0.0,
    "DollarsAtStake": 0.0,
    "legs": []
})

for row in rows_active:
    wid = row["WagerID"]
    ab = active_bets[wid]
    if ab["PotentialPayout"] == 0.0:
        ab["PotentialPayout"] = float(row["PotentialPayout"] or 0.0)
    if ab["DollarsAtStake"] == 0.0:
        ab["DollarsAtStake"] = float(row["DollarsAtStake"] or 0.0)
    ab["legs"].append({
        "EventType": row["EventType"],
        "EventLabel": row["EventLabel"],
        "ParticipantName": row["ParticipantName"]
    })

for wid, data in active_bets.items():
    pot   = data["PotentialPayout"]
    stake = data["DollarsAtStake"]
    legs  = data["legs"]

    dec_probs = []
    parlay_prob = 1.0
    for leg in legs:
        dec, prob = get_best_decimal_and_probability(
            leg["EventType"], leg["EventLabel"], leg["ParticipantName"]
        )
        dec_probs.append((dec, leg["EventType"], leg["EventLabel"]))
        parlay_prob *= prob

    if parlay_prob == 0:
        continue

    expected_payout = pot * parlay_prob
    sum_excess = sum((dec - 1.0) for dec,_,_ in dec_probs)
    if sum_excess <= 0:
        continue

    for dec, etype, elabel in dec_probs:
        frac = (dec - 1.0)/sum_excess
        active_stake_dict[(etype, elabel)] += frac * stake
        active_exp_dict[(etype, elabel)]   += frac * expected_payout

###############################################################################
# 7) Query #2: Realized => NetProfit for WLCA in (Win,Loss,Cashout)
###############################################################################
realized_np_dict = defaultdict(float)

cur_real = betting_conn.cursor(dictionary=True)
cur_real.execute("""
SELECT
    b.WagerID, b.NetProfit, b.WLCA, b.LegCount,
    l.LegID, l.ParticipantName, l.EventType, l.EventLabel
FROM bets b
JOIN legs l ON b.WagerID = l.WagerID
WHERE b.WhichBankroll='GreenAleph'
  AND l.LeagueName='NBA'
  AND b.WLCA IN ('Win','Loss','Cashout')
""")
rows_real = cur_real.fetchall()
cur_real.close()
betting_conn.close()

real_bets = defaultdict(lambda: {"WLCA": None, "NetProfit": 0.0, "legs":[]})
for row in rows_real:
    wid = row["WagerID"]
    rb = real_bets[wid]
    if rb["WLCA"] is None:
        rb["WLCA"] = row["WLCA"]
    if rb["NetProfit"] == 0.0 and row["NetProfit"] is not None:
        rb["NetProfit"] = float(row["NetProfit"] or 0.0)
    rb["legs"].append({
        "EventType": row["EventType"],
        "EventLabel": row["EventLabel"],
        "ParticipantName": row["ParticipantName"]
    })

for wid, info in real_bets.items():
    netp = info["NetProfit"]
    legs = info["legs"]

    dec_list = []
    for leg in legs:
        dec, prob = get_best_decimal_and_probability(
            leg["EventType"], leg["EventLabel"], leg["ParticipantName"]
        )
        dec_list.append((dec, leg["EventType"], leg["EventLabel"]))

    sum_excess = sum((d - 1.0) for d,_,_ in dec_list)
    if sum_excess<=0:
        continue

    for dec, etype, elabel in dec_list:
        frac = (dec - 1.0)/sum_excess
        realized_np_dict[(etype, elabel)] += frac*netp

###############################################################################
# 8) Combine results & compute `ExpectedValue`
#    = ActiveExpectedPayout - ActiveDollarsAtStake + RealizedNetProfit
###############################################################################
results_map = {}
all_keys = set(list(active_stake_dict.keys()) +
               list(active_exp_dict.keys()) +
               list(realized_np_dict.keys()))

for key in all_keys:
    stake_val  = active_stake_dict.get(key, 0.0)
    exp_val    = active_exp_dict.get(key, 0.0)
    real_net   = realized_np_dict.get(key, 0.0)
    # compute
    exp_value  = exp_val - stake_val + real_net
    results_map[key] = {
        "EventType": key[0],
        "EventLabel": key[1],
        "ActiveDollarsAtStake": round(stake_val,2),
        "ActiveExpectedPayout": round(exp_val,2),
        "RealizedNetProfit": round(real_net,2),
        "ExpectedValue": round(exp_value,2),
    }

# Convert dict -> DataFrame -> sort
df = pd.DataFrame(results_map.values()).sort_values(["EventType","EventLabel"])

# 9) Streamlit Display
st.subheader("Allocated Values by NBA Market")
st.markdown("Below table shows the four main columns for each NBA futures market:")
st.dataframe(df, use_container_width=True)

