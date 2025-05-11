import streamlit as st
import pymysql
import pandas as pd
import plotly.express as px
import re
from collections import defaultdict
from datetime import datetime, date

# ────────────────────────────────────────────────────────────────
# PAGE CONFIG
st.set_page_config(page_title="NBA Futures EV Trend", layout="wide")
st.title("📈 NBA Futures Expected Value Trend")
st.markdown("Track your portfolio’s total EV over time. Click **Snapshot EV** to record today’s value.")

# ────────────────────────────────────────────────────────────────
# DB CONNECTION HELPERS
def get_bet_conn():
    s = st.secrets["BETTING_DB"]
    return pymysql.connect(
        host=s["host"], user=s["user"], password=s["password"],
        database=s["database"], cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )

def get_fut_conn():
    s = st.secrets["FUTURES_DB"]
    return pymysql.connect(
        host=s["host"], user=s["user"], password=s["password"],
        database=s["database"], cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )

def with_cursor(conn):
    conn.ping(reconnect=True)
    return conn.cursor()

# ────────────────────────────────────────────────────────────────
# ODDS & MAPS (same as your EV table)
def american_odds_to_decimal(o):
    return 1 + (o/100) if o>0 else 1 + 100/abs(o) if o<0 else 1

def american_odds_to_prob(o):
    return 100/(o+100) if o>0 else abs(o)/(abs(o)+100) if o<0 else 0

def cast_odds(v):
    if v in (None,"",0): return 0
    if isinstance(v,(int,float)): return int(v)
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
    "Philadelphia 76ers":"76ers", "Milwaukee Bucks":"Bucks","Chicago Bulls":"Bulls",
    "Cleveland Cavaliers":"Cavaliers","Boston Celtics":"Celtics","Los Angeles Clippers":"Clippers",
    "Memphis Grizzlies":"Grizzlies","Atlanta Hawks":"Hawks","Miami Health":"Heat",
    "Charlotte Hornets":"Hornets","Utah Jazz":"Jazz","Sacramento Kings":"Kings",
    "New York Knicks":"Knicks","Los Angeles Lakers":"Lakers","Orlando Magic":"Magic",
    "Dallas Mavericks":"Mavericks","Brooklyn Nets":"Nets","Denver Nuggets":"Nuggets",
    "Indiana Pacers":"Pacers","New Orleans Pelicans":"Pelicans","Detroit Pistons":"Pistons",
    "Toronto Raptors":"Raptors","Houston Rockets":"Rockets","San Antonio Spurs":"Spurs",
    "Phoenix Suns":"Suns","Oklahoma City Thunder":"Thunder","Minnesota Timberwolves":"Timberwolves",
    "Portland Trail Blazers":"Trail Blazers","Golden State Warriors":"Warriors","Washington Wizards":"Wizards",
}

sportsbook_cols = ["BetMGM","DraftKings","Caesars","ESPNBet","FanDuel","BallyBet","RiversCasino","Bet365"]

def best_odds_decimal_prob(et, el, pn, asof, fut_conn):
    """Look back up to 100 rows, pick the 'longest' available odds, apply flat 5% vig."""
    vig = 0.05
    tbl = futures_table_map.get((et, el))
    if not tbl or fut_conn is None:
        return 1.0, 0.0
    alias = team_alias_map.get(pn, pn)
    cur = with_cursor(fut_conn)
    cur.execute(
        f"SELECT {','.join(sportsbook_cols)} FROM {tbl}"
        " WHERE team_name=%s AND date_created<=%s"
        " ORDER BY date_created DESC LIMIT 100",
        (alias, asof)
    )
    rows = cur.fetchall()
    cur.close()
    for r in rows:
        qs = [cast_odds(r[c]) for c in sportsbook_cols if cast_odds(r[c])]
        if not qs:
            continue
        # pick the one with smallest implied probability => longest shot
        best = min(qs, key=american_odds_to_prob)
        dec = american_odds_to_decimal(best)
        prob = american_odds_to_prob(best) * (1 - vig)
        return dec, prob
    return 1.0, 0.0

# ────────────────────────────────────────────────────────────────
# CALCULATE CURRENT TOTAL EV
def calculate_total_ev():
    bet_conn = get_bet_conn()
    fut_conn = get_fut_conn()
    now = datetime.utcnow()

    cur = with_cursor(bet_conn)

    # Active NBA futures
    cur.execute("""
      SELECT b.WagerID, b.PotentialPayout, b.DollarsAtStake,
             l.EventType, l.EventLabel, l.ParticipantName
        FROM bets b JOIN legs l ON b.WagerID=l.WagerID
       WHERE b.WhichBankroll='GreenAleph'
         AND b.WLCA='Active'
         AND l.LeagueName='NBA'
    """)
    active = defaultdict(lambda: {"pot":0,"stake":0,"legs":[]})
    for r in cur.fetchall():
        w = active[r["WagerID"]]
        w["pot"]   = w["pot"]   or float(r["PotentialPayout"] or 0)
        w["stake"] = w["stake"] or float(r["DollarsAtStake"] or 0)
        w["legs"].append((r["EventType"], r["EventLabel"], r["ParticipantName"]))

    total_active_stake = 0.0
    total_expected_payout = 0.0
    # allocate parlay EV proportionally
    for data in active.values():
        pot, stake, legs = data["pot"], data["stake"], data["legs"]
        decs, p = [], 1.0
        for et, el, pn in legs:
            dec, prob = best_odds_decimal_prob(et, el, pn, now, fut_conn)
            if prob == 0:
                p = 0
                break
            decs.append(dec)
            p *= prob
        if p == 0:
            continue
        total_active_stake += stake
        total_expected_payout += pot * p

    # Realized net profit (NBA futures)
    cur.execute("""
      SELECT b.WagerID, b.NetProfit,
             l.EventType, l.EventLabel, l.ParticipantName
        FROM bets b JOIN legs l ON b.WagerID=l.WagerID
       WHERE b.WhichBankroll='GreenAleph'
         AND b.WLCA IN ('Win','Loss','Cashout')
         AND l.LeagueName='NBA'
    """)
    realized = 0.0
    # distribute parlay legs proportionally
    nr = defaultdict(float)
    legs_map = defaultdict(list)
    for r in cur.fetchall():
        nr[r["WagerID"]] = float(r["NetProfit"] or 0)
        legs_map[r["WagerID"]].append((r["EventType"],r["EventLabel"],r["ParticipantName"]))
    for wid, legs in legs_map.items():
        net = nr[wid]
        decs = [best_odds_decimal_prob(et,el,pn,now,fut_conn)[0] for et,el,pn in legs]
        s = sum(d-1 for d in decs)
        if s <= 0:
            continue
        for d in decs:
            realized += net * ((d-1)/s)

    # Settled non-NBA bets
    cur.execute("""
      SELECT NetProfit
        FROM bets
       WHERE WhichBankroll='GreenAleph'
         AND WLCA IN ('Win','Loss','Cashout')
         AND EXISTS (
           SELECT 1 FROM legs l2 WHERE l2.WagerID=bets.WagerID AND l2.LeagueName<>'NBA'
         )
    """)
    for r in cur.fetchall():
        realized += float(r["NetProfit"] or 0)

    bet_conn.close()
    fut_conn.close()

    return total_expected_payout - total_active_stake + realized

# ────────────────────────────────────────────────────────────────
# SNAPSHOT & HISTORY
def snapshot_ev(total_ev):
    conn = get_bet_conn()
    cur = with_cursor(conn)
    cur.execute(
        "REPLACE INTO ev_history (snapshot_date, expected_value) VALUES (%s,%s)",
        (date.today(), total_ev)
    )
    conn.close()

def load_history():
    conn = get_bet_conn()
    df = pd.read_sql(
        "SELECT snapshot_date AS date, expected_value AS ev FROM ev_history ORDER BY snapshot_date",
        conn, parse_dates=["date"]
    )
    conn.close()
    return df

# ────────────────────────────────────────────────────────────────
# MAIN UI
try:
    total_ev = calculate_total_ev()
    st.metric("Current Total EV", f"${total_ev:,.2f}")
except Exception as e:
    st.error(f"Error calculating EV: {e}")
    total_ev = None

if st.button("🚀 Snapshot EV"):
    if total_ev is not None:
        snapshot_ev(total_ev)
        st.success("Saved snapshot for today.")
    else:
        st.warning("Cannot snapshot: EV calculation failed.")

# load & plot history
hist = load_history()
if not hist.empty:
    fig = px.line(hist, x="date", y="ev", markers=True,
                  title="Portfolio Expected Value Over Time")
    fig.update_layout(xaxis_title="Date", yaxis_title="Total EV ($)")
    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(hist.style.format({"ev":"${:,.2f}"}), height=300)
else:
    st.info("No EV history yet. Click **Snapshot EV** to start tracking.")

