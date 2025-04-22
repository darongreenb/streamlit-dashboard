import streamlit as st
import pymysql
from collections import defaultdict
import pandas as pd
import re
from datetime import datetime, timedelta

st.set_page_config(page_title="NBA Futures EV Dashboard", layout="wide")
st.title("NBA Futures: Active & Realized Payouts by Market")

# ----------- DB Connection Helpers --------------
def get_betting_conn():
    return pymysql.connect(
        host=st.secrets["BETTING_DB"]["host"],
        user=st.secrets["BETTING_DB"]["user"],
        password=st.secrets["BETTING_DB"]["password"],
        database=st.secrets["BETTING_DB"]["database"],
        cursorclass=pymysql.cursors.DictCursor
    )

def get_futures_df(table, futures_conn):
    with futures_conn.cursor() as cur:
        cur.execute(f"""
            SELECT * FROM {table}
            WHERE date_created >= '2024-01-01'
        """)
        df = pd.DataFrame(cur.fetchall())
        df['date_created'] = pd.to_datetime(df['date_created'])
        return df

# ----------- Odds Conversion Helpers ----------------
def american_odds_to_decimal(odds):
    return 1.0 + (odds / 100.0) if odds > 0 else 1.0 + (100.0 / abs(odds)) if odds != 0 else 1.0

def american_odds_to_probability(odds):
    return 100.0 / (odds + 100.0) if odds > 0 else abs(odds) / (abs(odds) + 100.0) if odds != 0 else 0.0

def safe_cast_odds(val):
    try:
        if isinstance(val, (int, float)): return int(val)
        if isinstance(val, str):
            m = re.search(r"[-+]?\d+", val)
            return int(m.group()) if m else 0
        return 0
    except: return 0

# ----------- Static Mappings ----------------
futures_table_map = {
    ("Most Valuable Player Award", "Award"): "NBAMVP",
    ("Defensive Player of Year Award", "Award"): "NBADefensivePotY",
    ("Most Improved Player Award", "Award"): "NBAMIP",
    ("Rookie of Year Award", "Award"): "NBARotY",
    ("Sixth Man of Year Award", "Award"): "NBASixthMotY",
    ("Championship", "NBA Championship"): "NBAChampionship",
    ("Conference Winner", "Eastern Conference"): "NBAEasternConference",
    ("Conference Winner", "Western Conference"): "NBAWesternConference",
    ("Division Winner", "Atlantic Division"): "NBAAtlantic",
    ("Division Winner", "Central Division"): "NBACentral",
    ("Division Winner", "Northwest Division"): "NBANorthwest",
    ("Division Winner", "Pacific Division"): "NBAPacific",
    ("Division Winner", "Southeast Division"): "NBASoutheast",
    ("Division Winner", "Southwest Division"): "NBASouthwest",
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
    "Portland Trail Blazers": "Trail Blazers", "Golden State Warriors": "Warriors", "Washington Wizards": "Wizards",
}

sportsbook_cols = ["BetMGM", "DraftKings", "Caesars", "ESPNBet", "FanDuel", "BallyBet", "RiversCasino", "Bet365"]

def get_best_odds_snapshot_df(event_type, event_label, futures_conn):
    table = futures_table_map.get((event_type, event_label))
    if not table:
        return pd.DataFrame()
    df = get_futures_df(table, futures_conn)
    df['best_american_odds'] = df[sportsbook_cols].apply(lambda row: max([safe_cast_odds(o) for o in row if safe_cast_odds(o) != 0], default=0), axis=1)
    return df[['team_name', 'date_created', 'best_american_odds']]

# ----------- Streamlit Logic ----------------
if st.button("Run MVP Return Chart"):
    conn_bets = get_betting_conn()
    conn_futures = get_betting_conn()
    
    # Fetch MVP Bets
    q = """
    SELECT b.WagerID, b.PotentialPayout, b.DollarsAtStake, b.DateTimePlaced,
           b.LegCount, l.LegID, l.ParticipantName, l.EventType, l.LeagueName
    FROM bets b
    JOIN legs l ON b.WagerID = l.WagerID
    WHERE b.WhichBankroll = 'GreenAleph'
      AND b.WLCA = 'Active'
      AND l.LeagueName = 'NBA'
    """
    with conn_bets.cursor() as cur:
        cur.execute(q)
        bet_rows = pd.DataFrame(cur.fetchall())

    # Clean & group
    bet_rows['DateTimePlaced'] = pd.to_datetime(bet_rows['DateTimePlaced'])
    bet_rows['LegID'] = bet_rows['LegID'].astype(str)
    bets_grouped = bet_rows.groupby('WagerID')

    mvp_odds = get_best_odds_snapshot_df("Most Valuable Player Award", "Award", conn_futures)
    mvp_odds.rename(columns={'team_name': 'ParticipantName'}, inplace=True)

    # Expand time series range
    date_range = pd.date_range(start=bet_rows['DateTimePlaced'].min().date(), end=datetime.today().date())

    results = []
    for snapshot_day in date_range:
        snapshot_ts = datetime.combine(snapshot_day, datetime.max.time())

        total_net, total_stake = 0.0, 0.0
        for wager_id, group in bets_grouped:
            if group['DateTimePlaced'].iloc[0] > snapshot_ts:
                continue

            pot = group['PotentialPayout'].iloc[0]
            stake = group['DollarsAtStake'].iloc[0]

            leg_info = []
            skip = False
            for _, row in group.iterrows():
                alias = team_alias_map.get(row['ParticipantName'], row['ParticipantName'])
                sub_odds = mvp_odds[(mvp_odds['ParticipantName'] == alias) & (mvp_odds['date_created'] <= snapshot_ts)]
                if sub_odds.empty:
                    skip = True
                    break
                latest_odds = sub_odds.sort_values('date_created').iloc[-1]['best_american_odds']
                if latest_odds == 0:
                    skip = True
                    break
                dec = american_odds_to_decimal(latest_odds)
                prob = american_odds_to_probability(latest_odds)
                leg_info.append((dec, prob, row['EventType']))

            if skip: continue

            parlay_prob = 1.0
            for _, p, _ in leg_info:
                parlay_prob *= p

            parlay_net = (pot * parlay_prob) - stake
            s_exc = sum(d - 1.0 for d, _, _ in leg_info)
            if s_exc <= 0: continue

            for d, _, etype in leg_info:
                frac = (d - 1.0) / s_exc
                if etype == "Most Valuable Player Award":
                    total_net += frac * parlay_net
                    total_stake += frac * stake

        pct_return = (total_net / total_stake * 100.0) if total_stake > 0 else 0.0
        results.append((snapshot_day, pct_return))

    df_result = pd.DataFrame(results, columns=["Date", "% Return"])
    st.line_chart(df_result.set_index("Date"))
