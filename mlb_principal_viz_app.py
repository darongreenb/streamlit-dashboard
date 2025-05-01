import streamlit as st
import pymysql
import re
from collections import defaultdict
from datetime import datetime
import pandas as pd

# ───────────────── PAGE CONFIG ─────────────────
st.set_page_config(page_title="NBA Futures EV Table", layout="wide")

# ───────────────── DB HELPERS ─────────────────
def new_betting_conn():
    return pymysql.connect(
        host       = "betting-db.cp86ssaw6cm7.us-east-1.rds.amazonaws.com",
        user       = "admin",
        password   = "7nRB1i2&A-K>",
        database   = "betting_db",
        cursorclass= pymysql.cursors.DictCursor,
        autocommit = True,
    )

def new_futures_conn():
    return pymysql.connect(
        host       = "greenalephfutures.cnwukek8ge3b.us-east-2.rds.amazonaws.com",
        user       = "admin",
        password   = "greenalephadmin",
        database   = "futuresdata",
        cursorclass= pymysql.cursors.DictCursor,
        autocommit = True,
    )

def with_cursor(conn):
    conn.ping(reconnect=True)
    return conn.cursor()

# ───────────────── ODDS HELPERS ─────────────────
def american_odds_to_decimal(o: int) -> float:
    return 1.0 + (o / 100) if o > 0 else 1.0 + 100 / abs(o) if o else 1.0

def american_odds_to_prob(o: int) -> float:
    return 100 / (o + 100) if o > 0 else abs(o) / (abs(o) + 100) if o else 0.0

def cast_odds(v):
    if v in (None, "", 0): return 0
    if isinstance(v, (int, float)): return int(v)
    m = re.search(r"[-+]?\d+", str(v))
    return int(m.group()) if m else 0

# ───────────────── EV TABLE PAGE ─────────────────
def ev_table_page():
    st.title("🧮 NBA Futures – Expected Value Table")
    st.markdown("""
    This table summarizes active bets and resolved outcomes for NBA futures markets. The **Expected Value** column combines active expected payouts and realized profits.
    """)

    bet_conn = new_betting_conn()
    fut_conn = new_futures_conn()
    now = datetime.utcnow()

    sql_active = """
        SELECT b.WagerID, b.PotentialPayout, b.DollarsAtStake,
               l.EventType, l.EventLabel, l.ParticipantName
          FROM bets b JOIN legs l ON b.WagerID = l.WagerID
         WHERE b.WhichBankroll='GreenAleph' AND b.WLCA='Active' AND l.LeagueName='NBA'
    """
    with with_cursor(bet_conn) as cur:
        cur.execute(sql_active)
        rows = cur.fetchall()

    active_bets = defaultdict(lambda: {"pot":0, "stake":0, "legs":[]})
    for r in rows:
        w = active_bets[r["WagerID"]]
        w["pot"] = w["pot"] or float(r["PotentialPayout"] or 0)
        w["stake"] = w["stake"] or float(r["DollarsAtStake"] or 0)
        w["legs"].append((r["EventType"], r["EventLabel"], r["ParticipantName"]))

    active_stake, active_exp = defaultdict(float), defaultdict(float)
    for data in active_bets.values():
        pot, stake, legs = data["pot"], data["stake"], data["legs"]
        decs = []; prob = 1.0
        for et, el, pn in legs:
            dec, p = 1.0, 0.0
            try:
                dec, p = best_odds_decimal_prob(et, el, pn, now, fut_conn)
            except:
                pass
            if p == 0: prob = 0; break
            decs.append((dec, et, el)); prob *= p
        if prob == 0: continue
        expected = pot * prob
        sum_exc = sum(d - 1 for d, _, _ in decs)
        if sum_exc <= 0: continue
        for d, et, el in decs:
            w = (d - 1) / sum_exc
            active_stake[(et, el)] += w * stake
            active_exp[(et, el)] += w * expected

    sql_real = """
        SELECT b.WagerID, b.NetProfit,
               l.EventType, l.EventLabel, l.ParticipantName
          FROM bets b JOIN legs l ON b.WagerID = l.WagerID
         WHERE b.WhichBankroll='GreenAleph'
           AND b.WLCA IN ('Win','Loss','Cashout')
           AND l.LeagueName='NBA'
    """
    with with_cursor(bet_conn) as cur:
        cur.execute(sql_real)
        rows = cur.fetchall()

    wager_net, wager_legs = defaultdict(float), defaultdict(list)
    for r in rows:
        wager_net[r["WagerID"]] = float(r["NetProfit"] or 0)
        wager_legs[r["WagerID"]].append((r["EventType"], r["EventLabel"], r["ParticipantName"]))

    realized_np = defaultdict(float)
    for wid, legs in wager_legs.items():
        net = wager_net[wid]
        decs = [(1.0, et, el) for et, el, pn in legs]
        try:
            decs = [(best_odds_decimal_prob(et, el, pn, now, fut_conn)[0], et, el) for et, el, pn in legs]
        except:
            pass
        sum_exc = sum(d - 1 for d, _, _ in decs)
        if sum_exc <= 0: continue
        for d, et, el in decs:
            realized_np[(et, el)] += net * ((d - 1) / sum_exc)

    bet_conn.close(); fut_conn.close()

    keys = set(active_stake) | set(active_exp) | set(realized_np)
    out = []
    for et, el in sorted(keys):
        stake = active_stake.get((et, el), 0)
        exp = active_exp.get((et, el), 0)
        net = realized_np.get((et, el), 0)
        out.append(dict(EventType=et, EventLabel=el,
                        ActiveDollarsAtStake=round(stake, 2),
                        ActiveExpectedPayout=round(exp, 2),
                        RealizedNetProfit=round(net, 2),
                        ExpectedValue=round(exp - stake + net, 2)))

    df = pd.DataFrame(out).sort_values(["EventType", "EventLabel"]).reset_index(drop=True)
    st.dataframe(df.style.format("{:.2f}").background_gradient(cmap="Greens", subset=["ExpectedValue"]))

# ───────────────── RUN ─────────────────
ev_table_page()
