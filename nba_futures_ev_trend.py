# ─────────────────────────────  ev_dashboard.py  ─────────────────────────────
import streamlit as st
import pandas as pd
import pymysql, re
from collections import defaultdict
from datetime import datetime, date
import plotly.express as px

# ─────────────────────────────  CONFIG  ─────────────────────────────
st.set_page_config(page_title="Futures EV Dashboard", layout="wide")
st.title("📈 GreenAleph – Futures Expected Value Over Time")

# ───────────────────  DB CONNECTION HELPERS  ───────────────────
def conn(section):
    s = st.secrets[section]
    return pymysql.connect(
        host=s["host"], user=s["user"], password=s["password"],
        database=s["database"], autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )

# Ensure our history table exists
with conn("FUTURES_DB") as c, c.cursor() as cur:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ev_history (
          snapshot_date DATE PRIMARY KEY,
          expected_value DOUBLE
        )
    """)

# ───────────────────  ODDS & MAPPINGS  ───────────────────
def american_odds_to_decimal(o):
    return 1 + (o/100) if o > 0 else 1 + 100/abs(o) if o else 1

def american_odds_to_prob(o):
    return 100/(o+100) if o > 0 else abs(o)/(abs(o)+100) if o else 0

def cast_odds(v):
    if v in (None, "", 0): return 0
    if isinstance(v, (int, float)): return int(v)
    m = re.search(r"[-+]?\d+", str(v))
    return int(m.group()) if m else 0

futures_table_map = {
    ("Championship","NBA Championship"):"NBAChampionship",
    ("Conference Winner","Eastern Conference"):"NBAEasternConference",
    ("Conference Winner","Western Conference"):"NBAWesternConference",
    ("Defensive Player of Year Award","Award"):"NBADefensivePotY",
    ("Division Winner","Atlantic Division"):"NBAAtlantic",
    ("Division Winner","Central Division"):"NBACentral",
    ("Division Winner","Northwest Division"):"NBANorthwest",
    ("Division Winner","Pacific Division"):"NBAPacific",
    ("Division Winner","Southeast Division"):"NBASoutheast",
    ("Division Winner","Southwest Division"):"NBASouthwest",
    ("Most Improved Player Award","Award"):"NBAMIP",
    ("Most Valuable Player Award","Award"):"NBAMVP",
    ("Rookie of Year Award","Award"):"NBARotY",
    ("Sixth Man of Year Award","Award"):"NBASixthMotY",
}

team_alias_map = {
    "Philadelphia 76ers":"76ers","Milwaukee Bucks":"Bucks","Chicago Bulls":"Bulls",
    "Cleveland Cavaliers":"Cavaliers","Boston Celtics":"Celtics","Los Angeles Clippers":"Clippers",
    "Memphis Grizzlies":"Grizzlies","Atlanta Hawks":"Hawks","Miami Heat":"Heat",
    "Charlotte Hornets":"Hornets","Utah Jazz":"Jazz","Sacramento Kings":"Kings",
    "New York Knicks":"Knicks","Los Angeles Lakers":"Lakers","Orlando Magic":"Magic",
    "Dallas Mavericks":"Mavericks","Brooklyn Nets":"Nets","Denver Nuggets":"Nuggets",
    "Indiana Pacers":"Pacers","New Orleans Pelicans":"Pelicans","Detroit Pistons":"Pistons",
    "Toronto Raptors":"Raptors","Houston Rockets":"Rockets","San Antonio Spurs":"Spurs",
    "Phoenix Suns":"Suns","Oklahoma City Thunder":"Thunder","Minnesota Timberwolves":"Timberwolves",
    "Portland Trail Blazers":"Trail Blazers","Golden State Warriors":"Warriors","Washington Wizards":"Wizards",
}

sportsbook_cols = ["BetMGM","DraftKings","Caesars","ESPNBet","FanDuel",
                   "BallyBet","RiversCasino","Bet365"]

def best_odds_decimal_prob(et, el, pn, cutoff, fut_conn, vig=0.05):
    tbl = futures_table_map.get((et, el))
    if not tbl or fut_conn is None:
        return 1.0, 0.0
    alias = team_alias_map.get(pn, pn)
    with fut_conn.cursor() as cur:
        cur.execute(
            f"""SELECT {','.join(sportsbook_cols)}
                 FROM {tbl}
                WHERE team_name=%s AND date_created<=%s
             ORDER BY date_created DESC
                LIMIT 100""",
            (alias, cutoff),
        )
        rows = cur.fetchall()
    for r in rows:
        quotes = [cast_odds(r.get(c)) for c in sportsbook_cols if cast_odds(r.get(c))]
        if not quotes:
            continue
        # pick the *longest* price = min implied probability
        best = min(quotes, key=american_odds_to_prob)
        dec  = american_odds_to_decimal(best)
        prob = american_odds_to_prob(best) * (1 - vig)
        return dec, prob
    return 1.0, 0.0

# ───────────────────  BUILD EV TABLE  ───────────────────
@st.cache_data(ttl=600)
def build_ev_table():
    bet_conn = conn("BETTING_DB")
    fut_conn = conn("FUTURES_DB")
    now = datetime.utcnow()
    vig_map = {k:0.05 for k in futures_table_map}

    #— fetch active NBA futures
    with bet_conn.cursor() as cur:
        cur.execute("""
            SELECT b.WagerID,b.PotentialPayout,b.DollarsAtStake,
                   l.EventType,l.EventLabel,l.ParticipantName
              FROM bets b JOIN legs l ON b.WagerID=l.WagerID
             WHERE b.WhichBankroll='GreenAleph'
               AND b.WLCA='Active'
               AND l.LeagueName='NBA'
        """)
        active_rows = cur.fetchall()

        # fetch settled NBA
        cur.execute("""
            SELECT b.WagerID,b.NetProfit,
                   l.EventType,l.EventLabel,l.ParticipantName
              FROM bets b JOIN legs l ON b.WagerID=l.WagerID
             WHERE b.WhichBankroll='GreenAleph'
               AND b.WLCA IN ('Win','Loss','Cashout')
               AND l.LeagueName='NBA'
        """)
        settled_rows = cur.fetchall()

        # settled non-NBA
        cur.execute("""
            SELECT b.NetProfit,l.LeagueName,l.EventType,l.EventLabel
              FROM bets b JOIN legs l ON b.WagerID=l.WagerID
             WHERE b.WhichBankroll='GreenAleph'
               AND b.WLCA IN ('Win','Loss','Cashout')
               AND l.LeagueName<>'NBA'
        """)
        other_rows = cur.fetchall()

        # wallet-wide net-profit for TOTAL row
        cur.execute("SELECT NetProfit FROM bets WHERE WhichBankroll='GreenAleph'")
        total_net = sum(float(r["NetProfit"] or 0) for r in cur.fetchall())

    # aggregate active
    active_bets = defaultdict(lambda: {"pot":0,"stake":0,"legs":[]})
    for r in active_rows:
        w = active_bets[r["WagerID"]]
        w["pot"]   = w["pot"]   or float(r["PotentialPayout"] or 0)
        w["stake"] = w["stake"] or float(r["DollarsAtStake"] or 0)
        w["legs"].append((r["EventType"], r["EventLabel"], r["ParticipantName"]))

    active_stake, active_exp = defaultdict(float), defaultdict(float)
    for data in active_bets.values():
        pot, stake, legs = data["pot"], data["stake"], data["legs"]
        decs, prob = [], 1.0
        for et, el, pn in legs:
            dec, p = best_odds_decimal_prob(et, el, pn, now, fut_conn, vig_map.get((et,el),0.05))
            if p == 0:
                prob = 0; break
            decs.append(dec); prob *= p
        if prob == 0: continue
        expected = pot * prob
        exc = sum(d-1 for d in decs)
        if exc <= 0: continue
        for d in decs:
            w = (d-1)/exc
            active_stake[(et,el)] += w*stake
            active_exp  [(et,el)] += w*expected

    # aggregate realized NBA
    wager_net, wager_legs = defaultdict(float), defaultdict(list)
    for r in settled_rows:
        wager_net[r["WagerID"]] = float(r["NetProfit"] or 0)
        wager_legs[r["WagerID"]].append((r["EventType"],r["EventLabel"],r["ParticipantName"]))

    realized_np = defaultdict(float)
    for wid, legs in wager_legs.items():
        net = wager_net[wid]
        decs = [best_odds_decimal_prob(et,el,pn,now,fut_conn,vig_map.get((et,el),0.05))[0] for et,el,pn in legs]
        exc = sum(d-1 for d in decs)
        if exc <= 0: continue
        for d,(et,el,_) in zip(decs, legs):
            realized_np[(et,el)] += net * ((d-1)/exc)

    # aggregate non-NBA settled
    other_by_key = defaultdict(float)
    for r in other_rows:
        key = (r["LeagueName"], r["EventType"], r["EventLabel"])
        other_by_key[key] += float(r["NetProfit"] or 0)

    # build DataFrame
    records = []
    # NBA rows
    for (et,el), tbl in futures_table_map.items():
        records.append({
            "LeagueName":"NBA",
            "EventType": et,
            "EventLabel": el,
            "ActiveDollarsAtStake": round(active_stake[(et,el)],2),
            "ActiveExpectedPayout": round(active_exp[(et,el)],2),
            "RealizedNetProfit": round(realized_np[(et,el)],2),
            "ExpectedValue": round(active_exp[(et,el)] - active_stake[(et,el)] + realized_np[(et,el)],2)
        })
    # other sports
    for (lg,et,el), net in other_by_key.items():
        records.append({
            "LeagueName":lg,
            "EventType": et,
            "EventLabel": el,
            "ActiveDollarsAtStake": 0.0,
            "ActiveExpectedPayout": 0.0,
            "RealizedNetProfit": round(net,2),
            "ExpectedValue": round(net,2)
        })

    df = pd.DataFrame(records).sort_values(["LeagueName","EventType","EventLabel"]).reset_index(drop=True)

    return df, total_net

# ─────────────────────────────  RENDER  ─────────────────────────────
df, wallet_np = build_ev_table()
st.subheader("Market-Level EV Table")
st.dataframe(df, use_container_width=True)

# Button to snapshot today's TOTAL EV
today = date.today()
if st.button("➕ Add latest snapshot"):
    total_ev = float(df.loc[df["LeagueName"]=="TOTAL","ExpectedValue"].sum())
    # but your TOTAL row isn't in df yet – so recompute:
    total_ev = float(df["ExpectedValue"].sum())
    with conn("FUTURES_DB") as c, c.cursor() as cur:
        cur.execute(
            "REPLACE INTO ev_history (snapshot_date, expected_value) VALUES (%s,%s)",
            (today, total_ev)
        )
    st.success(f"Saved snapshot {today}: ${total_ev:,.2f}")

# load & plot history
hist = pd.read_sql(
    "SELECT snapshot_date AS date, expected_value AS ev FROM ev_history ORDER BY date",
    conn("FUTURES_DB"), parse_dates=["date"]
)

st.subheader("EV Over Time")
if hist.empty:
    st.info("No snapshots yet. Hit the button above to record today's EV.")
else:
    fig = px.line(hist, x="date", y="ev",
                  markers=True,
                  labels={"date":"Date","ev":"Expected Value"})
    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(hist.style.format({"ev":"${:,.0f}"}), use_container_width=True)
