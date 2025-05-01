import streamlit as st
import pymysql, re
from collections import defaultdict
from datetime import datetime
import pandas as pd
from functools import lru_cache

# ────────────────────────── PAGE CONFIG ──────────────────────────
st.set_page_config(page_title="NBA Futures — EV Table", layout="wide")

# ────────────────────────── DB HELPERS ──────────────────────────

def new_betting_conn():
    return pymysql.connect(
        host="betting-db.cp86ssaw6cm7.us-east-1.rds.amazonaws.com",
        user="admin",
        password="7nRB1i2&A-K>",
        database="betting_db",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )


def new_futures_conn():
    return pymysql.connect(
        host="greenalephfutures.cnwukek8ge3b.us-east-2.rds.amazonaws.com",
        user="admin",
        password="greenalephadmin",
        database="futuresdata",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )


def with_cursor(conn):
    conn.ping(reconnect=True)
    return conn.cursor()

# ────────────────────────── ODDS UTILITIES ──────────────────────────

def american_odds_to_decimal(o: int) -> float:
    return 1 + (o / 100) if o > 0 else 1 + 100 / abs(o) if o else 1.0

def american_odds_to_prob(o: int) -> float:
    return 100 / (o + 100) if o > 0 else abs(o) / (abs(o) + 100) if o else 0.0

def cast_odds(val):
    if val in (None, "", 0):
        return 0
    if isinstance(val, (int, float)):
        return int(val)
    m = re.search(r"[-+]?\d+", str(val))
    return int(m.group()) if m else 0

# ────────────────────────── MAPPINGS ──────────────────────────

@lru_cache(maxsize=256)
def futures_table(event_type: str, event_label: str):
    mapping = {
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
    return mapping.get((event_type, event_label))

alias_map = {n: n.split()[-1] if n.startswith("Los") else n.split(maxsplit=1)[-1] for n in [
    "Atlanta Hawks", "Boston Celtics", "Brooklyn Nets", "Charlotte Hornets", "Chicago Bulls", "Cleveland Cavaliers",
    "Dallas Mavericks", "Denver Nuggets", "Detroit Pistons", "Golden State Warriors", "Houston Rockets", "Indiana Pacers",
    "Los Angeles Clippers", "Los Angeles Lakers", "Memphis Grizzlies", "Miami Heat", "Milwaukee Bucks",
    "Minnesota Timberwolves", "New Orleans Pelicans", "New York Knicks", "Oklahoma City Thunder", "Orlando Magic",
    "Philadelphia 76ers", "Phoenix Suns", "Portland Trail Blazers", "Sacramento Kings", "San Antonio Spurs", "Toronto Raptors",
    "Utah Jazz", "Washington Wizards"
]}

sportsbook_cols = ["BetMGM", "DraftKings", "Caesars", "ESPNBet", "FanDuel", "BallyBet", "RiversCasino", "Bet365"]

# single‑leg helper
def best_odds_decimal_prob(event_type, event_label, participant, cutoff_dt, fut_conn):
    tbl = futures_table(event_type, event_label)
    if not tbl:
        return 1.0, 0.0
    alias = alias_map.get(participant, participant)
    with with_cursor(fut_conn) as cur:
        cur.execute(
            f"SELECT {','.join(sportsbook_cols)} FROM {tbl} WHERE team_name=%s AND date_created<=%s ORDER BY date_created DESC LIMIT 1",
            (alias, cutoff_dt),
        )
        row = cur.fetchone()
    if not row:
        return 1.0, 0.0
    nums = [cast_odds(row[c]) for c in sportsbook_cols if cast_odds(row[c])]
    if not nums:
        return 1.0, 0.0
    best = max(nums)
    return american_odds_to_decimal(best), american_odds_to_prob(best)

# ────────────────────────── MAIN PAGE ──────────────────────────

def ev_table_page():
    st.title("📊 NBA Futures – Expected Value Table")

    bet_conn, fut_conn = new_betting_conn(), new_futures_conn()
    now = datetime.utcnow()

    # ---------- ACTIVE wagers ----------
    with with_cursor(bet_conn) as cur:
        cur.execute(
            """SELECT b.WagerID, b.PotentialPayout, b.DollarsAtStake,
                          l.EventType, l.EventLabel, l.ParticipantName
                     FROM bets b JOIN legs l ON b.WagerID = l.WagerID
                    WHERE b.WhichBankroll='GreenAleph' AND b.WLCA='Active' AND l.LeagueName='NBA'"""
        )
        active_rows = cur.fetchall()

    active_by_wager = defaultdict(lambda: {"pot": 0.0, "stake": 0.0, "legs": []})
    for r in active_rows:
        d = active_by_wager[r["WagerID"]]
        d["pot"] = d["pot"] or float(r["PotentialPayout"] or 0)
        d["stake"] = d["stake"] or float(r["DollarsAtStake"] or 0)
        d["legs"].append((r["EventType"], r["EventLabel"], r["ParticipantName"]))

    active_stake, active_exp = defaultdict(float), defaultdict(float)
    for rec in active_by_wager.values():
        pot, stake, legs = rec["pot"], rec["stake"], rec["legs"]
        prob = 1.0; leg_info = []
        for et, el, pn in legs:
            dec, p = best_odds_decimal_prob(et, el, pn, now, fut_conn)
            if p == 0:
                prob = 0
                break
            leg_info.append((dec, et, el))
            prob *= p
        if prob == 0 or not leg_info:
            continue
        expected = pot * prob
        denom = sum(d - 1 for d, _, _ in leg_info)
        if denom <= 0:
            continue
        for dec, et, el in leg_info:
            w = (dec - 1) / denom
            active_stake[(et, el)] += w * stake
            active_exp[(et, el)] += w * expected

    # ---------- RESOLVED wagers ----------
    with with_cursor(bet_conn) as cur:
        cur.execute(
            """SELECT b.WagerID, b.NetProfit,
                          l.EventType, l.EventLabel, l.ParticipantName
                     FROM bets b JOIN legs l ON b.WagerID = l.WagerID
                    WHERE b.WhichBankroll='GreenAleph' AND l.LeagueName='NBA' AND b.WLCA IN ('Win','Loss','Cashout')"""
        )
        resolved_rows = cur.fetchall()

    wager_net, wager_legs = defaultdict(float), defaultdict(list)
    for r in resolved_rows:
        wager_net[r["WagerID"]] = float(r["NetProfit"] or 0)
        wager_legs[r["WagerID"]].append((r["EventType"], r["EventLabel"], r["ParticipantName"]))

    realized_np = defaultdict(float)
    for wid, legs in wager_legs.items():
        net_profit = wager_net[wid]
        legs_info = [(best_odds_decimal_prob(et, el, pn, now, fut_conn)[0], et, el) for et, el, pn in legs]
        denom = sum(d - 1 for d, _, _ in legs_info)
        if denom <= 0:
            continue
        for dec, et, el in legs_info:
            realized_np[(et, el)] += net_profit * ((dec - 1) / denom)

    bet_conn.close(); fut_conn.close()

    # ---------- Assemble table ----------
    markets = sorted(set(active_stake) | set(active_exp) | set(realized_np))
    rows = []]
    for et, el in markets:
        stake   = active_stake.get((et
        stake   = active_stake.get((et, el), 0.0)
        payout  = active_exp.get((et, el), 0.0)
        realized= realized_np.get((et, el), 0.0)
        rows.append({
            "EventType": et,
            "EventLabel": el,
            "Active Stake": round(stake, 2),
            "Expected Payout": round(payout, 2),
            "Realized Profit": round(realized, 2),
            "Expected Value": round(payout - stake + realized, 2),
        })

    if not rows:
        st.info("No NBA futures data to display.")
        return

    df = pd.DataFrame(rows).sort_values(["EventType", "EventLabel"]).reset_index(drop=True)

    numeric_cols = [c for c in df.columns if c not in ("EventType", "EventLabel")]

    styled = (
        df.style.format("{:.2f}", subset=numeric_cols)
               .background_gradient(cmap="Greens", subset=["Expected Value"], vmin=df["Expected Value"].min(), vmax=df["Expected Value"].max())
               .highlight_max(axis=0, color="#D6EAF8")
    )

    st.dataframe(styled, use_container_width=True)

# ────────────────────────── RUN ──────────────────────────

ev_table_page()
