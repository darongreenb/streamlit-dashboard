import streamlit as st
import mysql.connector
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from collections import defaultdict

# Retrieve secrets
db_host = st.secrets["DB_HOST"]
db_user = st.secrets["DB_USER"]
db_password = st.secrets["DB_PASSWORD"]
db_name = st.secrets["DB_NAME"]

futures_host = st.secrets["FUTURES_DB"]["host"]
futures_user = st.secrets["FUTURES_DB"]["user"]
futures_password = st.secrets["FUTURES_DB"]["password"]
futures_db = st.secrets["FUTURES_DB"]["database"]

# Odds conversion

def american_odds_to_decimal(odds):
    return 1.0 + (odds / 100.0) if odds > 0 else 1.0 + (100.0 / abs(odds))

def american_odds_to_probability(odds):
    return 100.0 / (odds + 100.0) if odds > 0 else abs(odds) / (abs(odds) + 100.0)

# Table map
futures_table_map = {
    "Most Valuable Player Award": "NBAMVP",
    "Defensive Player of the Year Award": "NBADefensivePotY",
    "Most Improved Player Award": "NBAMIP",
    "Rookie of the Year Award": "NBARotY",
    "Sixth Man of Year Award": "NBASixthMotY"
}

# Connect to DBs
conn_bets = mysql.connector.connect(host=db_host, user=db_user, password=db_password, database=db_name)
conn_futures = mysql.connector.connect(host=futures_host, user=futures_user, password=futures_password, database=futures_db)

# Function to get latest odds as of date
def get_latest_odds_as_of_date(event_type, participant, snapshot_datetime):
    table = futures_table_map.get(event_type)
    if not table:
        return 0
    cursor = conn_futures.cursor(dictionary=True)
    query = f"""
        SELECT FanDuel FROM {table}
        WHERE team_name = %s AND date_created <= %s
        ORDER BY date_created DESC LIMIT 1
    """
    cursor.execute(query, (participant, snapshot_datetime))
    row = cursor.fetchone()
    cursor.close()
    return int(row["FanDuel"]) if row and row["FanDuel"] else 0

# Query all NBA bets
cursor = conn_bets.cursor(dictionary=True)
cursor.execute("""
    SELECT b.WagerID, b.PotentialPayout, b.DollarsAtStake, b.DateTimePlaced, b.LegCount,
           l.LegID, l.ParticipantName, l.EventType, l.LeagueName
    FROM bets b
    JOIN legs l ON b.WagerID = l.WagerID
    WHERE b.WhichBankroll = 'GreenAleph' AND l.LeagueName = 'NBA'
""")
rows = cursor.fetchall()
cursor.close()

# Organize bets
bets = defaultdict(lambda: {
    "PotentialPayout": 0.0, "DollarsAtStake": 0.0, "DateTimePlaced": None,
    "LegCount": 0, "legs": []
})

for row in rows:
    w = bets[row["WagerID"]]
    if w["PotentialPayout"] == 0 and row["PotentialPayout"]: w["PotentialPayout"] = float(row["PotentialPayout"])
    if w["DollarsAtStake"] == 0 and row["DollarsAtStake"]: w["DollarsAtStake"] = float(row["DollarsAtStake"])
    if not w["DateTimePlaced"] and row["DateTimePlaced"]: w["DateTimePlaced"] = row["DateTimePlaced"]
    if w["LegCount"] == 0 and row["LegCount"]: w["LegCount"] = row["LegCount"]
    w["legs"].append({
        "LegID": row["LegID"], "ParticipantName": row["ParticipantName"],
        "EventType": row["EventType"], "LeagueName": row["LeagueName"]
    })

# Daily loop
time_series = []
start_date = datetime(2025, 3, 20)
end_date = datetime(2025, 4, 1)
current_date = start_date

while current_date <= end_date:
    snapshot = current_date.replace(hour=23, minute=59, second=59)
    mvp_net, mvp_stake = 0.0, 0.0
    for w in bets.values():
        if w["DateTimePlaced"] > snapshot:
            continue
        pot, stake = w["PotentialPayout"], w["DollarsAtStake"]
        parlay_prob = 1.0
        leg_odds = []
        for leg in w["legs"]:
            odds = get_latest_odds_as_of_date(leg["EventType"], leg["ParticipantName"], snapshot)
            dec = american_odds_to_decimal(odds) if odds else 1.0
            prob = american_odds_to_probability(odds) if odds else 0.0
            parlay_prob *= prob
            leg_odds.append((dec, leg["EventType"]))

        if parlay_prob == 0:
            continue
        parlay_net = (pot * parlay_prob) - stake
        sum_excess = sum((d - 1.0) for d, _ in leg_odds)
        if sum_excess <= 0:
            continue
        for dec, etype in leg_odds:
            if etype == "Most Valuable Player Award":
                frac = (dec - 1.0) / sum_excess
                mvp_net += frac * parlay_net
                mvp_stake += frac * stake
    pct_return = (mvp_net / mvp_stake) * 100.0 if mvp_stake > 0 else 0.0
    time_series.append((current_date.date(), pct_return))
    current_date += timedelta(days=1)

# Plotting
st.title("NBA MVP Daily % Return (GA1)")
dates, values = zip(*time_series)
fig, ax = plt.subplots(figsize=(10, 6))
ax.plot(dates, values, marker='o')
ax.set_title("Daily % Return for NBA MVP (GreenAleph)")
ax.set_xlabel("Date")
ax.set_ylabel("Return (%)")
ax.grid(True)
plt.xticks(rotation=45)
st.pyplot(fig)

# Close connections
conn_bets.close()
conn_futures.close()
