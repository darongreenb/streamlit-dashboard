import streamlit as st
import pymysql, re
from collections import defaultdict
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# ────────── PAGE CONFIG ──────────
st.set_page_config(page_title="NBA Futures Dashboard", layout="wide")

# ────────── DB HELPERS ──────────

def new_betting_conn():
    return pymysql.connect(
        host=st.secrets["BETTING_DB"]["host"],
        user=st.secrets["BETTING_DB"]["user"],
        password=st.secrets["BETTING_DB"]["password"],
        database=st.secrets["BETTING_DB"]["database"],
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )

def new_futures_conn():
    return pymysql.connect(
        host=st.secrets["FUTURES_DB"]["host"],
        user=st.secrets["FUTURES_DB"]["user"],
        password=st.secrets["FUTURES_DB"]["password"],
        database=st.secrets["FUTURES_DB"]["database"],
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )

def with_cursor(conn):
    conn.ping(reconnect=True)
    return conn.cursor()

# ────────── ODDS HELPERS ──────────

def american_odds_to_decimal(o:int)->float:
    return 1 + (o/100) if o>0 else 1 + 100/abs(o) if o else 1

def american_odds_to_prob(o:int)->float:
    return 100/(o+100) if o>0 else abs(o)/(abs(o)+100) if o else 0

def cast_odds(v):
    if v in (None, "", 0):
        return 0
    if isinstance(v, (int, float)):
        return int(v)
    m = re.search(r"[-+]?\d+", str(v))
    return int(m.group()) if m else 0

# ────────── MAPS ──────────
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
    "Atlanta Hawks": "Hawks", "Boston Celtics": "Celtics", "Brooklyn Nets": "Nets",
    "Charlotte Hornets": "Hornets", "Chicago Bulls": "Bulls", "Cleveland Cavaliers": "Cavaliers",
    "Dallas Mavericks": "Mavericks", "Denver Nuggets": "Nuggets", "Detroit Pistons": "Pistons",
    "Golden State Warriors": "Warriors", "Houston Rockets": "Rockets", "Indiana Pacers": "Pacers",
    "LA Clippers": "Clippers", "Los Angeles Clippers": "Clippers", "Los Angeles Lakers": "Lakers",
    "Memphis Grizzlies": "Grizzlies", "Miami Heat": "Heat", "Milwaukee Bucks": "Bucks",
    "Minnesota Timberwolves": "Timberwolves", "New Orleans Pelicans": "Pelicans",
    "New York Knicks": "Knicks", "Oklahoma City Thunder": "Thunder", "Orlando Magic": "Magic",
    "Philadelphia 76ers": "76ers", "Phoenix Suns": "Suns", "Portland Trail Blazers": "Trail Blazers",
    "Sacramento Kings": "Kings", "San Antonio Spurs": "Spurs", "Toronto Raptors": "Raptors",
    "Utah Jazz": "Jazz", "Washington Wizards": "Wizards"
}

sportsbook_cols = ["BetMGM", "DraftKings", "Caesars", "ESPNBet", "FanDuel",
                   "BallyBet", "RiversCasino", "Bet365"]

# ────────── CORE ──────────

def best_odds_decimal_prob(event_type, event_label, participant, cutoff_dt, fut_conn):
    tbl = futures_table_map.get((event_type, event_label))
    if not tbl:
        return 1.0, 0.0
    alias = team_alias_map.get(participant, participant)
    with with_cursor(fut_conn) as cur:
        cur.execute(
            f"SELECT {','.join(sportsbook_cols)}\n               FROM {tbl}\n              WHERE team_name=%s AND date_created<=%s\n              ORDER BY date_created DESC LIMIT 1",
            (alias, cutoff_dt))
        row = cur.fetchone()
    if not row:
        return 1.0, 0.0
    nums = [cast_odds(row[c]) for c in sportsbook_cols if cast_odds(row[c])]
    if not nums:
        return 1.0, 0.0
    best = max(nums)
    return american_odds_to_decimal(best), american_odds_to_prob(best)

# ────────── EV TABLE ──────────

def build_ev_dataframe(now: datetime):
    bet_conn, fut_conn = new_betting_conn(), new_futures_conn()

    # --- Active bets
    with with_cursor(bet_conn) as cur:
        cur.execute("""
            SELECT b.WagerID, b.PotentialPayout, b.DollarsAtStake,
                   l.EventType, l.EventLabel, l.ParticipantName
              FROM bets b JOIN legs l ON b.WagerID=l.WagerID
             WHERE b.WhichBankroll='GreenAleph' AND b.WLCA='Active' AND l.LeagueName='NBA'""")
        active_rows = cur.fetchall()

    active_bets = defaultdict(lambda: {"pot":0, "stake":0, "legs":[]})
    for r in active_rows:
        d = active_bets[r["WagerID"]]
        d["pot"]   = d["pot"]   or float(r["PotentialPayout"] or 0)
        d["stake"] = d["stake"] or float(r["DollarsAtStake"] or 0)
        d["legs"].append((r["EventType"], r["EventLabel"], r["ParticipantName"]))

    active_stake, active_exp = defaultdict(float), defaultdict(float)
    for pot, stake, legs in [(v["pot"], v["stake"], v["legs"]) for v in active_bets.values()]:
        decs, prob = [], 1.0
        for et, el, pn in legs:
            dec, p = best_odds_decimal_prob(et, el, pn, now, fut_conn)
            if p == 0:
                prob = 0; break
            decs.append((dec, et, el)); prob *= p
        if prob == 0:
            continue
        expected = pot * prob
        sum_exc = sum(d-1 for d,_,_ in decs)
        if sum_exc <= 0:
            continue
        for d, et, el in decs:
            w = (d-1)/sum_exc
            active_stake[(et,el)] += w*stake
            active_exp[(et,el)]   += w*expected

    # --- Resolved bets
    with with_cursor(bet_conn) as cur:
        cur.execute("""
            SELECT b.WagerID, b.NetProfit,
                   l.EventType, l.EventLabel, l.ParticipantName
              FROM bets b JOIN legs l ON b.WagerID=l.WagerID
             WHERE b.WhichBankroll='GreenAleph' AND b.WLCA IN ('Win','Loss','Cashout')
               AND l.LeagueName='NBA'""")
        res_rows = cur.fetchall()

    wager_net, wager_legs = defaultdict(float), defaultdict(list)
    for r in res_rows:
        wager_net[r["WagerID"]] = float(r["NetProfit"] or 0)
        wager_legs[r["WagerID"]].append((r["EventType"], r["EventLabel"], r["ParticipantName"]))

    realized_np = defaultdict(float)
    for wid, legs in wager_legs.items():
        net = wager_net[wid]
        decs = [(best_odds_decimal_prob(et,el,pn,now,fut_conn)[0], et, el) for et,el,pn in legs]
        sum_exc = sum(d-1 for d,_,_ in decs)
        if sum_exc <= 0:
            continue
        for d, et, el in decs:
            realized_np[(et,el)] += net * ((d-1)/sum_exc)

    bet_conn.close(); fut_conn.close()

    keys = set(active_stake) | set(active_exp) | set(realized_np)
    out=[]
