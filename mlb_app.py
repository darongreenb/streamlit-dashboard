import streamlit as st
import mysql.connector
import pandas as pd
from collections import defaultdict

# Retrieve primary DB (betting_db) credentials
db_host = st.secrets["DB_HOST"]
db_user = st.secrets["DB_USER"]
db_password = st.secrets["DB_PASSWORD"]
db_name = st.secrets["DB_NAME"]

# Retrieve futures DB credentials
futures_host = st.secrets["FUTURES_DB"]["host"]
futures_user = st.secrets["FUTURES_DB"]["user"]
futures_password = st.secrets["FUTURES_DB"]["password"]
futures_db = st.secrets["FUTURES_DB"]["database"]

def american_odds_to_decimal(odds):
    if odds == 0:
        return 1.0
    elif odds > 0:
        return 1.0 + (odds / 100.0)
    else:
        return 1.0 + (100.0 / abs(odds))

def american_odds_to_probability(odds):
    if odds == 0:
        return 0.0
    elif odds > 0:
        return 100.0 / (odds + 100.0)
    else:
        return abs(odds) / (abs(odds) + 100.0)

# Mapping of (EventType, EventLabel) to futures DB table
futures_table_map = {
    ("Championship", "NBA Championship"): "NBAChampionship",
    ("Conference Winner", "Eastern Conference"): "NBAEasternConference",
    ("Conference Winner", "Western Conference"): "NBAWesternConference",
    ("Defensive Player of Year Award", "Award"): "NBADefensivePotY",
    ("Division Winner", "Pacific Division"): "NBAPacific",
    ("Division Winner", "Southwest Division"): "NBASouthwest",
    ("Division Winner", "Southeast Division"): "NBASoutheast",
    ("Division Winner", "Atlantic Division"): "NBAAtlantic",
    ("Division Winner", "Central Division"): "NBACentral",
    ("Division Winner", "Northwest Division"): "NBANorthwest",
    ("Most Improved Player Award", "Award"): "NBAMIP",
    ("Most Valuable Player Award", "Award"): "NBAMVP",
    ("Rookie of Year Award", "Award"): "NBARotY",
    ("Sixth Man of Year Award", "Award"): "NBASixthMotY",
}

# Query all relevant NBA futures bets
query = """
    SELECT b.WagerID, b.PotentialPayout, b.DollarsAtStake, b.LegCount, b.DateTimePlaced,
           l.LegID, l.ParticipantName, l.EventType, l.EventLabel, l.LeagueName
    FROM bets b
    JOIN legs l ON b.WagerID = l.WagerID
    WHERE b.WhichBankroll = 'GreenAleph' AND l.LeagueName = 'NBA'
"""

try:
    conn = mysql.connector.connect(host=db_host, user=db_user, password=db_password, database=db_name)
    cursor = conn.cursor(dictionary=True)
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
except mysql.connector.Error as err:
    st.error(f"Error retrieving NBA bets: {err}")
    st.stop()

wager_dict = defaultdict(lambda: {
    "PotentialPayout": 0.0,
    "DollarsAtStake": 0.0,
    "LegCount": 0,
    "DateTimePlaced": None,
    "legs": []
})

for row in rows:
    w_id = row["WagerID"]
    try:
        if wager_dict[w_id]["PotentialPayout"] == 0.0 and row["PotentialPayout"] is not None:
            wager_dict[w_id]["PotentialPayout"] = float(row["PotentialPayout"])
        if wager_dict[w_id]["DollarsAtStake"] == 0.0 and row["DollarsAtStake"] is not None:
            wager_dict[w_id]["DollarsAtStake"] = float(row["DollarsAtStake"])
        if wager_dict[w_id]["LegCount"] == 0 and row["LegCount"] is not None:
            wager_dict[w_id]["LegCount"] = row["LegCount"]
        if wager_dict[w_id]["DateTimePlaced"] is None and row["DateTimePlaced"]:
            wager_dict[w_id]["DateTimePlaced"] = row["DateTimePlaced"]
    except (ValueError, TypeError):
        continue  # skip problematic rows

    wager_dict[w_id]["legs"].append({
        "LegID": row["LegID"],
        "ParticipantName": row["ParticipantName"],
        "EventType": row["EventType"],
        "EventLabel": row["EventLabel"]
    })

# Helper to fetch latest odds
def get_latest_odds(event_type, event_label, participant):
    table = futures_table_map.get((event_type, event_label))
    if not table:
        return 1.0, 0.0
    try:
        conn = mysql.connector.connect(
            host=futures_host,
            user=futures_user,
            password=futures_password,
            database=futures_db
        )
        cursor = conn.cursor(dictionary=True)
        cursor.execute(f"""
            SELECT FanDuel FROM {table}
            WHERE team_name = %s
            ORDER BY date_created DESC
            LIMIT 1
        """, (participant,))
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        if row and row["FanDuel"] is not None:
            try:
                odds = int(row["FanDuel"])
                dec = american_odds_to_decimal(odds)
                prob = american_odds_to_probability(odds)
                return dec, prob
            except (ValueError, TypeError):
                return 1.0, 0.0
    except:
        return 1.0, 0.0
    return 1.0, 0.0

# Compute EV by (EventType, EventLabel)
result_by_market = defaultdict(float)

for w_id, w_data in wager_dict.items():
    pot = w_data["PotentialPayout"]
    stake = w_data["DollarsAtStake"]
    legs = w_data["legs"]

    parlay_prob = 1.0
    leg_odds_info = []

    for leg in legs:
        dec, prob = get_latest_odds(leg["EventType"], leg["EventLabel"], leg["ParticipantName"])
        parlay_prob *= prob
        leg_odds_info.append((dec, leg["EventType"], leg["EventLabel"]))

    if parlay_prob == 0:
        continue

    net = (pot * parlay_prob) - stake
    sum_excess = sum((dec - 1.0) for dec, _, _ in leg_odds_info)
    if sum_excess <= 0:
        continue

    for dec, etype, elabel in leg_odds_info:
        frac = (dec - 1.0) / sum_excess
        result_by_market[(etype, elabel)] += frac * net

# Display Results
st.title("NBA Expected Net Profit by Futures Market (GA1)")

sorted_results = sorted(result_by_market.items(), key=lambda x: x[1], reverse=True)

df = pd.DataFrame([{
    "EventType": k[0],
    "EventLabel": k[1],
    "ExpectedNetProfit": round(v, 2)
} for k, v in sorted_results])

st.dataframe(df)
