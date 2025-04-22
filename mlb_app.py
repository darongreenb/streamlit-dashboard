import streamlit as st
import mysql.connector
from collections import defaultdict
import pandas as pd
import re

# ─── Streamlit Setup ───────────────────────────────────────────────────────────
st.set_page_config(layout="wide")
st.title("NBA Futures EV Dashboard")

# ─── Database Connections ──────────────────────────────────────────────────────
@st.cache_resource
def get_db_connections():
    betting_conn = mysql.connector.connect(
        host=st.secrets["DB_HOST"],
        user=st.secrets["DB_USER"],
        password=st.secrets["DB_PASSWORD"],
        database=st.secrets["DB_NAME"]
    )
    fdb = st.secrets["FUTURES_DB"]
    futures_conn = mysql.connector.connect(
        host=fdb["host"],
        user=fdb["user"],
        password=fdb["password"],
        database=fdb["database"]
    )
    return betting_conn, futures_conn

betting_conn, futures_conn = get_db_connections()

# ─── Odds Conversion ───────────────────────────────────────────────────────────
def american_odds_to_decimal(odds):
    if odds == 0: return 1.0
    return 1 + (odds / 100.0) if odds > 0 else 1 + (100.0 / abs(odds))

def american_odds_to_probability(odds):
    if odds == 0: return 0.0
    return 100.0 / (odds + 100.0) if odds > 0 else abs(odds) / (abs(odds) + 100.0)

def safe_cast_odds(val):
    try:
        if isinstance(val, (int, float)): return int(val)
        if isinstance(val, str):
            m = re.search(r"[-+]?\d+", val)
            return int(m.group()) if m else 0
    except: pass
    return 0

# ─── Mappings ──────────────────────────────────────────────────────────────────
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
    ("Sixth Man of Year Award", "Award"): "NBASixthMotY"
}

team_alias_map = {
    "Philadelphia 76ers": "76ers", "Milwaukee Bucks": "Bucks", "Chicago Bulls": "Bulls",
    "Cleveland Cavaliers": "Cavaliers", "Boston Celtics": "Celtics", "Los Angeles Clippers": "Clippers",
    "Memphis Grizzlies": "Grizzlies", "Atlanta Hawks": "Hawks", "Miami Heat": "Heat",
    "Charlotte Hornets": "Hornets", "Utah Jazz": "Jazz", "Sacramento Kings": "Kings",
    "New York Knicks": "Knicks", "Los Angeles Lakers": "Lakers", "Orlando Magic": "Magic",
    "Dallas Mavericks": "Mavericks", "Brooklyn Nets": "Nets", "Denver Nuggets": "Nuggets",
    "Indiana Pacers": "Pacers", "New Orleans Pelicans": "Pelicans", "Detroit Pistons": "Pistons",
    "Toronto Raptors": "Raptors", "Houston Rockets": "Rockets", "San Antonio Spurs": "Spurs",
    "Phoenix Suns": "Suns", "Oklahoma City Thunder": "Thunder", "Minnesota Timberwolves": "Timberwolves",
    "Portland Trail Blazers": "Trail Blazers", "Golden State Warriors": "Warriors", "Washington Wizards": "Wizards"
}

sportsbook_cols = ["BetMGM", "DraftKings", "Caesars", "ESPNBet", "FanDuel", "BallyBet", "RiversCasino", "Bet365"]

def get_best_odds(event_type, event_label, participant):
    table = futures_table_map.get((event_type, event_label))
    if not table:
        return 1.0, 0.0
    alias = team_alias_map.get(participant, participant)

    try:
        cursor = futures_conn.cursor(dictionary=True)
        cursor.execute(f"""
            SELECT {','.join(sportsbook_cols)}
            FROM {table}
            WHERE team_name = %s
            ORDER BY date_created DESC
        """, (alias,))
        rows = cursor.fetchall()
        cursor.close()

        for row in rows:
            odds = [safe_cast_odds(row[col]) for col in sportsbook_cols]
            odds = [o for o in odds if o != 0]
            if odds:
                best = max(odds)
                return american_odds_to_decimal(best), american_odds_to_probability(best)
    except:
        pass
    return 1.0, 0.0

# ─── Query 1: Active → Stake & Expected Payout ─────────────────────────────────
stake_dict = defaultdict(float)
exp_payout_dict = defaultdict(float)

cur = betting_conn.cursor(dictionary=True)
cur.execute("""
    SELECT b.WagerID, b.PotentialPayout, b.DollarsAtStake, b.LegCount,
           l.ParticipantName, l.EventType, l.EventLabel
    FROM bets b JOIN legs l ON b.WagerID = l.WagerID
    WHERE b.WhichBankroll = 'GreenAleph' AND l.LeagueName = 'NBA' AND b.WLCA = 'Active'
""")
rows = cur.fetchall()
cur.close()

grouped = defaultdict(lambda: {"Pot": 0.0, "Stake": 0.0, "legs": []})
for r in rows:
    g = grouped[r["WagerID"]]
    g["Pot"] = r["PotentialPayout"]
    g["Stake"] = r["DollarsAtStake"]
    g["legs"].append((r["EventType"], r["EventLabel"], r["ParticipantName"]))

for v in grouped.values():
    legs = v["legs"]
    probs = []
    parlay_prob = 1.0
    for et, el, p in legs:
        dec, prob = get_best_odds(et, el, p)
        parlay_prob *= prob
        probs.append((dec, et, el))

    if parlay_prob == 0:
        continue

    sum_excess = sum(d - 1.0 for d, _, _ in probs)
    if sum_excess <= 0:
        continue

    for dec, et, el in probs:
        frac = (dec - 1.0) / sum_excess
        stake_dict[(et, el)] += frac * v["Stake"]
        exp_payout_dict[(et, el)] += frac * (v["Pot"] * parlay_prob)

# ─── Query 2: Realized Net Profit ──────────────────────────────────────────────
net_profit_dict = defaultdict(float)

cur = betting_conn.cursor(dictionary=True)
cur.execute("""
    SELECT b.WagerID, b.NetProfit, b.LegCount,
           l.ParticipantName, l.EventType, l.EventLabel
    FROM bets b JOIN legs l ON b.WagerID = l.WagerID
    WHERE b.WhichBankroll = 'GreenAleph' AND l.LeagueName = 'NBA'
          AND b.WLCA IN ('Win','Loss','Cashout')
""")
rows = cur.fetchall()
cur.close()

grouped2 = defaultdict(lambda: {"Net": 0.0, "legs": []})
for r in rows:
    g = grouped2[r["WagerID"]]
    g["Net"] = r["NetProfit"]
    g["legs"].append((r["EventType"], r["EventLabel"], r["ParticipantName"]))

for v in grouped2.values():
    legs = v["legs"]
    probs = []
    for et, el, p in legs:
        dec, _ = get_best_odds(et, el, p)
        probs.append((dec, et, el))

    sum_excess = sum(d - 1.0 for d, _, _ in probs)
    if sum_excess <= 0:
        continue

    for dec, et, el in probs:
        frac = (dec - 1.0) / sum_excess
        net_profit_dict[(et, el)] += frac * v["Net"]

# ─── Combine Results ──────────────────────────────────────────────────────────
all_keys = set(stake_dict) | set(exp_payout_dict) | set(net_profit_dict)
rows = []
for k in sorted(all_keys):
    stake = stake_dict.get(k, 0.0)
    exp = exp_payout_dict.get(k, 0.0)
    real = net_profit_dict.get(k, 0.0)
    rows.append({
        "EventType": k[0],
        "EventLabel": k[1],
        "ActiveDollarsAtStake": round(stake, 2),
        "ActiveExpectedPayout": round(exp, 2),
        "RealizedNetProfit": round(real, 2),
        "ExpectedValue": round(exp - stake + real, 2)
    })

# ─── Streamlit Display ────────────────────────────────────────────────────────
df = pd.DataFrame(rows)
st.dataframe(df, use_container_width=True)
