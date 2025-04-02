import streamlit as st
import mysql.connector
import pandas as pd
from collections import defaultdict

# Odds conversion helpers
def american_odds_to_decimal(odds: int) -> float:
    if odds == 0:
        return 1.0
    return 1.0 + (odds / 100.0) if odds > 0 else 1.0 + (100.0 / abs(odds))

def american_odds_to_probability(odds: int) -> float:
    if odds == 0:
        return 0.0
    return 100.0 / (odds + 100.0) if odds > 0 else abs(odds) / (abs(odds) + 100.0)

# Secrets: primary (betting) DB
betting_host = st.secrets["DB_HOST"]
betting_user = st.secrets["DB_USER"]
betting_pw   = st.secrets["DB_PASSWORD"]
betting_db   = st.secrets["DB_NAME"]

# Secrets: futures DB
futures_host = st.secrets["FUTURES_DB"]["host"]
futures_user = st.secrets["FUTURES_DB"]["user"]
futures_pw   = st.secrets["FUTURES_DB"]["password"]
futures_db   = st.secrets["FUTURES_DB"]["database"]

# Mapping of (EventType, EventLabel) -> table
event_label_table_map = {
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
    ("Sixth Man of Year Award", "Award"): "NBASixthMotY"
}

# Helper to connect and get data from a DB
def get_data_from_db(query, params=None, db_source="betting"):
    conn = None
    try:
        if db_source == "betting":
            conn = mysql.connector.connect(
                host=betting_host, user=betting_user, password=betting_pw, database=betting_db)
        else:
            conn = mysql.connector.connect(
                host=futures_host, user=futures_user, password=futures_pw, database=futures_db)

        cur = conn.cursor(dictionary=True)
        cur.execute(query, params)
        results = cur.fetchall()
        cur.close()
        conn.close()
        return results
    except Exception as e:
        st.error(f"Database error: {e}")
        if conn:
            conn.close()
        return None

# Page starts here
if "NBA Expected Performance" in st.sidebar.radio("Go to", ["NBA Expected Performance"]):
    st.title("NBA Expected Performance (GA1)")
    st.write("Up-to-date expected net profit (EV - stake) for each NBA futures market in the GreenAleph bankroll.")

    # Fetch relevant bets/legs
    query = """
        SELECT b.WagerID, b.PotentialPayout, b.DollarsAtStake, b.LegCount, b.DateTimePlaced,
               l.LegID, l.ParticipantName, l.EventType, l.EventLabel, l.LeagueName
        FROM bets b
        JOIN legs l ON b.WagerID = l.WagerID
        WHERE b.WhichBankroll = 'GreenAleph'
          AND l.LeagueName = 'NBA'
    """
    rows = get_data_from_db(query)
    if not rows:
        st.warning("No NBA futures bets found.")
        st.stop()

    # Group all bets by WagerID
    wager_dict = defaultdict(lambda: {"PotentialPayout": 0.0, "DollarsAtStake": 0.0, "legs": []})
    for row in rows:
        w_id = row["WagerID"]
        wager_dict[w_id]["PotentialPayout"] = float(row["PotentialPayout"])
        wager_dict[w_id]["DollarsAtStake"] = float(row["DollarsAtStake"])
        wager_dict[w_id]["legs"].append(row)

    # Aggregate EV by (EventType, EventLabel)
    ev_summary = defaultdict(float)

    for w_id, data in wager_dict.items():
        pot = data["PotentialPayout"]
        stake = data["DollarsAtStake"]
        legs = data["legs"]

        parlay_prob = 1.0
        leg_infos = []  # (decimal_odds, probability, event_type, event_label)

        for leg in legs:
            key = (leg["EventType"], leg["EventLabel"])
            tbl = event_label_table_map.get(key)
            if not tbl:
                leg_infos.append((1.0, 0.0, leg["EventType"], leg["EventLabel"]))
                continue
            query_odds = f"""
                SELECT FanDuel FROM {tbl}
                WHERE team_name = %s
                ORDER BY date_created DESC
                LIMIT 1
            """
            result = get_data_from_db(query_odds, (leg["ParticipantName"],), db_source="futures")
            if result and result[0]["FanDuel"] is not None:
                try:
                    odds = int(result[0]["FanDuel"])
                    dec = american_odds_to_decimal(odds)
                    prob = american_odds_to_probability(odds)
                except:
                    dec, prob = 1.0, 0.0
            else:
                dec, prob = 1.0, 0.0
            leg_infos.append((dec, prob, leg["EventType"], leg["EventLabel"]))
            parlay_prob *= prob

        if parlay_prob == 0:
            continue

        parlay_net = (pot * parlay_prob) - stake
        sum_excess = sum((d[0] - 1.0) for d in leg_infos)
        if sum_excess <= 0:
            continue

        for dec, _, et, el in leg_infos:
            fraction = (dec - 1.0) / sum_excess
            ev_summary[(et, el)] += fraction * parlay_net

    # Display result
    ev_df = pd.DataFrame([
        {"EventType": et, "EventLabel": el, "ExpectedNetProfit": ev}
        for (et, el), ev in ev_summary.items()
    ])
    ev_df = ev_df.sort_values("ExpectedNetProfit", ascending=False)
    ev_df["ExpectedNetProfit"] = ev_df["ExpectedNetProfit"].round(2)

    st.dataframe(ev_df, use_container_width=True)
