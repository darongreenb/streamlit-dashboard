import streamlit as st
import mysql.connector
from collections import defaultdict
import pandas as pd

# Database credentials from Streamlit secrets
db_host = st.secrets["DB_HOST"]
db_user = st.secrets["DB_USER"]
db_password = st.secrets["DB_PASSWORD"]
db_name = st.secrets["DB_NAME"]

futures_host = st.secrets["FUTURES_DB"]["host"]
futures_user = st.secrets["FUTURES_DB"]["user"]
futures_password = st.secrets["FUTURES_DB"]["password"]
futures_db = st.secrets["FUTURES_DB"]["database"]

# Helper: Convert American odds
def american_odds_to_decimal(odds: int) -> float:
    if odds == 0: return 1.0
    elif odds > 0: return 1.0 + (odds / 100)
    else: return 1.0 + (100 / abs(odds))

def american_odds_to_probability(odds: int) -> float:
    if odds == 0: return 0.0
    elif odds > 0: return 100 / (odds + 100)
    else: return abs(odds) / (abs(odds) + 100)

# Helper: Get latest odds from futures DB
def get_latest_odds(event_type: str, participant: str) -> (float, float):
    table_map = {
        "Most Valuable Player Award": "NBAMVP",
        "Defensive Player of the Year Award": "NBADefensivePotY",
        "Most Improved Player Award": "NBAMIP",
        "Rookie of the Year Award": "NBARotY",
        "Sixth Man of Year Award": "NBASixthMotY",
    }
    table = table_map.get(event_type)
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
        cursor.execute(
            f"""
            SELECT FanDuel FROM {table}
            WHERE team_name = %s
            ORDER BY date_created DESC
            LIMIT 1
            """, (participant,)
        )
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        if row and row["FanDuel"] is not None:
            odds = int(row["FanDuel"])
            return american_odds_to_decimal(odds), american_odds_to_probability(odds)
    except:
        pass
    return 1.0, 0.0

# Page layout
st.title("NBA Expected Net Profit by Market (GA1)")
st.write("The table below shows expected net profit by EventType and EventLabel for all NBA futures in the GreenAleph bankroll.")

# Step 1: Retrieve all distinct (EventType, EventLabel) combinations
conn = mysql.connector.connect(
    host=db_host, user=db_user, password=db_password, database=db_name
)
cursor = conn.cursor(dictionary=True)
cursor.execute("""
    SELECT DISTINCT l.EventType, l.EventLabel
    FROM bets b
    JOIN legs l ON b.WagerID = l.WagerID
    WHERE b.WhichBankroll = 'GreenAleph'
      AND l.LeagueName = 'NBA'
""")
markets = cursor.fetchall()
cursor.close()
conn.close()

# Step 2: Compute EV for each market
results = []
for market in markets:
    event_type = market["EventType"]
    event_label = market["EventLabel"]

    # Get relevant bets
    conn = mysql.connector.connect(
        host=db_host, user=db_user, password=db_password, database=db_name
    )
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT b.WagerID, b.PotentialPayout, b.DollarsAtStake, b.LegCount,
               l.LegID, l.ParticipantName, l.EventType, l.EventLabel
        FROM bets b
        JOIN legs l ON b.WagerID = l.WagerID
        WHERE b.WhichBankroll = 'GreenAleph'
          AND l.LeagueName = 'NBA'
          AND l.EventType = %s
          AND l.EventLabel = %s
    """, (event_type, event_label))
    base_rows = cursor.fetchall()
    cursor.close()
    conn.close()

    if not base_rows:
        continue

    wager_ids = {r["WagerID"] for r in base_rows}
    in_clause = ",".join(f"'{wid}'" for wid in wager_ids)

    conn = mysql.connector.connect(
        host=db_host, user=db_user, password=db_password, database=db_name
    )
    cursor = conn.cursor(dictionary=True)
    cursor.execute(f"""
        SELECT b.WagerID, b.PotentialPayout, b.DollarsAtStake, b.LegCount,
               l.LegID, l.ParticipantName, l.EventType, l.EventLabel
        FROM bets b
        JOIN legs l ON b.WagerID = l.WagerID
        WHERE b.WagerID IN ({in_clause})
          AND b.WhichBankroll = 'GreenAleph'
    """)
    all_legs = cursor.fetchall()
    cursor.close()
    conn.close()

    bet_dict = defaultdict(lambda: {"PotentialPayout": 0.0, "DollarsAtStake": 0.0, "Legs": []})
    for row in all_legs:
        wid = row["WagerID"]
        bet_dict[wid]["PotentialPayout"] = float(row["PotentialPayout"])
        bet_dict[wid]["DollarsAtStake"] = float(row["DollarsAtStake"])
        bet_dict[wid]["Legs"].append(row)

    total_ev = 0.0
    for data in bet_dict.values():
        pot = data["PotentialPayout"]
        stake = data["DollarsAtStake"]
        legs = data["Legs"]

        parlay_prob = 1.0
        dec_list = []

        for leg in legs:
            dec, prob = get_latest_odds(leg["EventType"], leg["ParticipantName"])
            parlay_prob *= prob
            dec_list.append((dec, leg["EventType"], leg["EventLabel"]))

        if parlay_prob == 0:
            continue

        parlay_net = (pot * parlay_prob) - stake
        sum_excess = sum((d[0] - 1.0) for d in dec_list)
        if sum_excess <= 0:
            continue

        for dec, et, el in dec_list:
            if et == event_type and el == event_label:
                fraction = (dec - 1.0) / sum_excess
                total_ev += fraction * parlay_net

    results.append({
        "EventType": event_type,
        "EventLabel": event_label,
        "ExpectedNetProfit": round(total_ev, 2)
    })

# Step 3: Display in table
if results:
    df = pd.DataFrame(results).sort_values(by=["EventType", "ExpectedNetProfit"], ascending=[True, False])
    st.dataframe(df, use_container_width=True)
else:
    st.write("No relevant bets found.")
