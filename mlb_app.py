import streamlit as st
import mysql.connector
import pandas as pd
from collections import defaultdict

# === Secrets ===
db_host = st.secrets["DB_HOST"]
db_user = st.secrets["DB_USER"]
db_password = st.secrets["DB_PASSWORD"]
db_name = st.secrets["DB_NAME"]

futures_host = st.secrets["FUTURES_DB"]["host"]
futures_user = st.secrets["FUTURES_DB"]["user"]
futures_password = st.secrets["FUTURES_DB"]["password"]
futures_db = st.secrets["FUTURES_DB"]["database"]

# === Odds Conversion ===
def american_odds_to_decimal(odds):
    if odds == 0:
        return 1.0
    return 1.0 + (odds / 100.0) if odds > 0 else 1.0 + (100.0 / abs(odds))

def american_odds_to_probability(odds):
    if odds == 0:
        return 0.0
    return 100.0 / (odds + 100.0) if odds > 0 else abs(odds) / (abs(odds) + 100.0)

# === Table Map for Odds Source ===
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

# === Fetch Betting Data ===
with st.spinner("Fetching betting data..."):
    try:
        query = """
            SELECT b.WagerID, b.PotentialPayout, b.DollarsAtStake, b.LegCount, b.DateTimePlaced,
                   l.LegID, l.ParticipantName, l.EventType, l.EventLabel, l.LeagueName
            FROM bets b
            JOIN legs l ON b.WagerID = l.WagerID
            WHERE b.WhichBankroll = 'GreenAleph' AND l.LeagueName = 'NBA'
            LIMIT 1000
        """
        conn = mysql.connector.connect(
            host=db_host, user=db_user, password=db_password, database=db_name
        )
        cursor = conn.cursor(dictionary=True)
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
    except Exception as e:
        st.error(f"Error fetching betting data: {e}")
        st.stop()

# === Organize Bets ===
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
        if wager_dict[w_id]["PotentialPayout"] == 0.0 and row["PotentialPayout"]:
            wager_dict[w_id]["PotentialPayout"] = float(row["PotentialPayout"])
        if wager_dict[w_id]["DollarsAtStake"] == 0.0 and row["DollarsAtStake"]:
            wager_dict[w_id]["DollarsAtStake"] = float(row["DollarsAtStake"])
        if wager_dict[w_id]["LegCount"] == 0 and row["LegCount"]:
            wager_dict[w_id]["LegCount"] = row["LegCount"]
        if wager_dict[w_id]["DateTimePlaced"] is None and row["DateTimePlaced"]:
            wager_dict[w_id]["DateTimePlaced"] = row["DateTimePlaced"]
        wager_dict[w_id]["legs"].append({
            "LegID": row["LegID"],
            "ParticipantName": row["ParticipantName"],
            "EventType": row["EventType"],
            "EventLabel": row["EventLabel"]
        })
    except Exception as e:
        st.warning(f"Row skipped due to error: {e}")

# === Fetch Odds ===
def get_latest_odds(event_type, event_label, participant):
    table = futures_table_map.get((event_type, event_label))
    if not table:
        return 1.0, 0.0
    try:
        conn = mysql.connector.connect(
            host=futures_host, user=futures_user,
            password=futures_password, database=futures_db
        )
        cursor = conn.cursor(dictionary=True)
        cursor.execute(f"""
            SELECT FanDuel FROM {table}
            WHERE team_name = %s
            ORDER BY date_created DESC LIMIT 1
        """, (participant,))
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        if row and row["FanDuel"] is not None:
            odds = int(row["FanDuel"])
            return american_odds_to_decimal(odds), american_odds_to_probability(odds)
    except Exception as e:
        st.warning(f"Failed to fetch odds for {participant}: {e}")
    return 1.0, 0.0

# === Compute EV by Market ===
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
        fraction = (dec - 1.0) / sum_excess
        result_by_market[(etype, elabel)] += fraction * net

# === Display Table ===
st.title("NBA Expected Net Profit by Futures Market (GA1)")

if not result_by_market:
    st.warning("No data found to compute expected net profit.")
else:
    df = pd.DataFrame([
        {"EventType": k[0], "EventLabel": k[1], "ExpectedNetProfit": round(v, 2)}
        for k, v in sorted(result_by_market.items())
    ])
    st.dataframe(df, use_container_width=True)
