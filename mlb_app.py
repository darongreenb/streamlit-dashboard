import streamlit as st
import pymysql
from collections import defaultdict
import pandas as pd
import re

st.set_page_config(page_title="NBA Futures EV Dashboard", layout="wide")
st.title("NBA Futures: Active & Realized Payouts by Market")

# ----------- DB Connection Helpers (Fresh each time) --------------
def get_betting_conn():
    return pymysql.connect(
        host=st.secrets["BETTING_DB"]["host"],
        user=st.secrets["BETTING_DB"]["user"],
        password=st.secrets["BETTING_DB"]["password"],
        database=st.secrets["BETTING_DB"]["database"],
        cursorclass=pymysql.cursors.DictCursor
    )

def get_futures_conn():
    return pymysql.connect(
        host=st.secrets["FUTURES_DB"]["host"],
        user=st.secrets["FUTURES_DB"]["user"],
        password=st.secrets["FUTURES_DB"]["password"],
        database=st.secrets["FUTURES_DB"]["database"],
        cursorclass=pymysql.cursors.DictCursor
    )

# ----------- Odds helpers ----------------
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

# ----------- Mappings ----------------
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

def get_best_decimal_and_probability(event_type, event_label, participant, futures_conn):
    table = futures_table_map.get((event_type, event_label))
    if not table:
        return 1.0, 0.0
    alias = team_alias_map.get(participant, participant)
    with futures_conn.cursor() as cur:
        cur.execute(f"""
            SELECT {','.join(sportsbook_cols)}
            FROM {table}
            WHERE team_name = %s
            ORDER BY date_created DESC
        """, (alias,))
        rows = cur.fetchall()
    for row in rows:
        odds = [safe_cast_odds(row[col]) for col in sportsbook_cols]
        nonzero = [o for o in odds if o != 0]
        if nonzero:
            best = max(nonzero)
            return american_odds_to_decimal(best), american_odds_to_probability(best)
    return 1.0, 0.0

# ----------- Main processing ----------------

def main():
    betting_conn = get_betting_conn()
    futures_conn = get_futures_conn()

    # --- Query 1: Active Bets ---
    active_bets = defaultdict(lambda: {"pot": 0.0, "stake": 0.0, "legs": []})
    with betting_conn.cursor() as cur:
        cur.execute("""
            SELECT b.WagerID, b.PotentialPayout, b.DollarsAtStake,
                   l.EventType, l.EventLabel, l.ParticipantName
            FROM bets b
            JOIN legs l ON b.WagerID = l.WagerID
            WHERE b.WhichBankroll = 'GreenAleph' AND b.WLCA = 'Active' AND l.LeagueName = 'NBA'
        """)
        for row in cur.fetchall():
            bet = active_bets[row["WagerID"]]
            bet["pot"] = bet["pot"] or float(row["PotentialPayout"] or 0.0)
            bet["stake"] = bet["stake"] or float(row["DollarsAtStake"] or 0.0)
            bet["legs"].append((row["EventType"], row["EventLabel"], row["ParticipantName"]))

    active_stake, active_payout = defaultdict(float), defaultdict(float)
    for b in active_bets.values():
        pot, stake, legs = b["pot"], b["stake"], b["legs"]
        probs, decs = [], []
        for et, el, pn in legs:
            d, p = get_best_decimal_and_probability(et, el, pn, futures_conn)
            probs.append(p)
            decs.append((d, et, el))
        if 0 in probs:
            continue
        prob = 1.0
        for p in probs:
            prob *= p
        expected = pot * prob
        sum_excess = sum(d - 1.0 for d, _, _ in decs)
        if sum_excess <= 0:
            continue
        for d, et, el in decs:
            frac = (d - 1.0) / sum_excess
            active_stake[(et, el)] += frac * stake
            active_payout[(et, el)] += frac * expected

    # --- Query 2: Realized Net Profit ---
    realized_net = defaultdict(float)
    with betting_conn.cursor() as cur:
        cur.execute("""
            SELECT b.WagerID, b.NetProfit, l.EventType, l.EventLabel, l.ParticipantName
            FROM bets b
            JOIN legs l ON b.WagerID = l.WagerID
            WHERE b.WhichBankroll = 'GreenAleph'
              AND b.WLCA IN ('Win','Loss','Cashout')
              AND l.LeagueName = 'NBA'
        """)
        results = cur.fetchall()

    realized_legs = defaultdict(list)
    for r in results:
        realized_legs[r["WagerID"]].append((r["EventType"], r["EventLabel"], r["ParticipantName"], float(r["NetProfit"] or 0.0)))

    for legs in realized_legs.values():
        net = legs[0][3]
        decs = [(get_best_decimal_and_probability(et, el, pn, futures_conn)[0], et, el) for et, el, pn, _ in legs]
        s_exc = sum(d - 1.0 for d, _, _ in decs)
        if s_exc <= 0:
            continue
        for d, et, el in decs:
            realized_net[(et, el)] += net * ((d - 1.0) / s_exc)

    # --- Final Output ---
    records = []
    all_keys = set(active_stake) | set(active_payout) | set(realized_net)
    for k in sorted(all_keys):
        stake = active_stake.get(k, 0.0)
        payout = active_payout.get(k, 0.0)
        net = realized_net.get(k, 0.0)
        ev = payout - stake + net
        records.append({
            "EventType": k[0],
            "EventLabel": k[1],
            "ActiveDollarsAtStake": round(stake, 2),
            "ActiveExpectedPayout": round(payout, 2),
            "RealizedNetProfit": round(net, 2),
            "ExpectedValue": round(ev, 2),
        })

    df = pd.DataFrame(records).sort_values(["EventType", "EventLabel"]).reset_index(drop=True)
    st.dataframe(df, use_container_width=True)

main()
