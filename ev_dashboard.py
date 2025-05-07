# ─────────────────────  NBA Futures EV Dashboard (Streamlit)  ─────────────────────
"""
ev_dashboard.py
Run with:  streamlit run ev_dashboard.py
Requires `.streamlit/secrets.toml` holding [betting_db] and [futures_db] creds.
"""

import streamlit as st
import pandas as pd, re, pymysql
from collections import defaultdict
from datetime import datetime, date
from typing import Dict, Tuple

# ╭──────────────────────  DB HELPERS  ──────────────────────╮
def _conn(section: str):
    s = st.secrets[section]
    return pymysql.connect(
        host=s.host,
        user=s.user,
        password=s.password,
        database=s.database,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
        connect_timeout=10,
    )

def _with_cursor(cnx):               # auto‑reconnect helper
    cnx.ping(reconnect=True)
    return cnx.cursor()
# ╰───────────────────────────────────────────────────────────╯


# ╭───────────────────  MAPS & CONSTANTS  ───────────────────╮
futures_table_map: Dict[Tuple[str, str], str] = {
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

team_alias_map: Dict[str, str] = {
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

sportsbook_cols = [
    "BetMGM","DraftKings","Caesars","ESPNBet","FanDuel",
    "BallyBet","RiversCasino","Bet365",
]
# ╰───────────────────────────────────────────────────────────╯


# ╭────────────────────  ODDS HELPERS  ─────────────────────╮
def _american_to_decimal(o):  return 1 + (o/100) if o > 0 else 1 + 100/abs(o) if o else 1
def _american_to_prob(o):     return 100/(o+100) if o > 0 else abs(o)/(abs(o)+100) if o else 0
def _cast_odds(v):
    if v in (None, "", 0): return 0
    if isinstance(v, (int, float)): return int(v)
    m = re.search(r"[-+]?\d+", str(v))
    return int(m.group()) if m else 0
# ╰──────────────────────────────────────────────────────────╯


# ╭───────────────────  SNAPSHOT FUNCTIONS  ─────────────────╮
def _save_snapshot(total_ev: float):
    """Upsert today's EV into ev_history."""
    with _conn("futures_db") as cnx, cnx.cursor() as cur:
        cur.execute(
            "CREATE TABLE IF NOT EXISTS ev_history ("
            "snapshot_date DATE PRIMARY KEY, expected_value DECIMAL(14,2) NOT NULL)"
        )
        cur.execute(
            "REPLACE INTO ev_history (snapshot_date, expected_value) VALUES (%s,%s)",
            (date.today(), total_ev),
        )

def _load_history() -> pd.DataFrame:
    with _conn("futures_db") as cnx:
        return pd.read_sql(
            "SELECT snapshot_date AS date, expected_value AS ev "
            "FROM ev_history ORDER BY date",
            cnx,
            parse_dates=["date"],
        )
# ╰──────────────────────────────────────────────────────────╯


# ╭─────────────────────  CORE LOGIC  ───────────────────────╮
def _best_odds(event_type, event_label, participant, cutoff_dt, fut_cnx, vig_map):
    tbl = futures_table_map.get((event_type, event_label))
    if not tbl:
        return 1.0, 0.0
    alias = team_alias_map.get(participant, participant)
    cur = _with_cursor(fut_cnx)
    cur.execute(
        f"""SELECT {','.join(sportsbook_cols)}
               FROM {tbl}
              WHERE team_name=%s AND date_created<=%s
          ORDER BY date_created DESC LIMIT 100""",
        (alias, cutoff_dt),
    )
    rows = cur.fetchall()
    cur.close()
    for r in rows:
        quotes = [_cast_odds(r[c]) for c in sportsbook_cols if _cast_odds(r[c])]
        if quotes:
            best = min(quotes, key=_american_to_prob)
            dec  = _american_to_decimal(best)
            prob = _american_to_prob(best) * (1 - vig_map.get((event_type, event_label), 0.05))
            return dec, prob
    return 1.0, 0.0


def build_ev_table() -> pd.DataFrame:
    bet_cnx, fut_cnx = _conn("betting_db"), _conn("futures_db")
    now = datetime.utcnow()
    vig = {k: 0.05 for k in futures_table_map}

    cur = _with_cursor(bet_cnx)

    # ---------- ACTIVE NBA FUTURES ----------
    cur.execute("""
        SELECT b.WagerID, b.PotentialPayout, b.DollarsAtStake,
               l.EventType, l.EventLabel, l.ParticipantName
          FROM bets b JOIN legs l ON b.WagerID=l.WagerID
         WHERE b.WhichBankroll='GreenAleph'
           AND b.WLCA='Active'
           AND l.LeagueName='NBA'""")
    active_rows = cur.fetchall()

    active, act_stake, act_exp = defaultdict(lambda: {"pot": 0, "stake": 0, "legs": []}), defaultdict(float), defaultdict(float)
    for r in active_rows:
        w = active[r["WagerID"]]
        w["pot"] = w["pot"] or float(r["PotentialPayout"] or 0)
        w["stake"] = w["stake"] or float(r["DollarsAtStake"] or 0)
        w["legs"].append((r["EventType"], r["EventLabel"], r["ParticipantName"]))

    for data in active.values():
        pot, stake, legs = data.values()
        decs, prob = [], 1.0
        for et, el, pn in legs:
            dec, p = _best_odds(et, el, pn, now, fut_cnx, vig)
            if p == 0:
                prob = 0
                break
            decs.append(dec)
            prob *= p
        if prob == 0:
            continue
        expected = pot * prob
        exc_sum = sum(d - 1 for d in decs)
        if exc_sum <= 0:
            continue
        for d in decs:
            w = (d - 1) / exc_sum
            act_stake[(et, el)] += w * stake
            act_exp[(et, el)] += w * expected

    # ---------- REALIZED NBA FUTURES ----------
    cur.execute("""
        SELECT b.WagerID, b.NetProfit,
               l.EventType, l.EventLabel, l.ParticipantName
          FROM bets b JOIN legs l ON b.WagerID=l.WagerID
         WHERE b.WhichBankroll='GreenAleph'
           AND b.WLCA IN ('Win','Loss','Cashout')
           AND l.LeagueName='NBA'""")
    realized_rows = cur.fetchall()
    wager_net, wager_legs = defaultdict(float), defaultdict(list)
    for r in realized_rows:
        wager_net[r["WagerID"]] = float(r["NetProfit"] or 0)
        wager_legs[r["WagerID"]].append((r["EventType"], r["EventLabel"], r["ParticipantName"]))
    realized = defaultdict(float)
    for wid, legs in wager_legs.items():
        net = wager_net[wid]
        decs = [_best_odds(et, el, pn, now, fut_cnx, vig)[0] for et, el, pn in legs]
        exc_sum = sum(d - 1 for d in decs)
        if exc_sum <= 0:
            continue
        for d, (et, el, _) in zip(decs, legs):
            realized[(et, el)] += net * ((d - 1) / exc_sum)

    # ---------- COMPLETED OTHER SPORTS ----------
    cur.execute("""
        SELECT b.NetProfit,
               l.EventType, l.EventLabel, l.LeagueName
          FROM bets b JOIN legs l ON b.WagerID=l.WagerID
         WHERE b.WhichBankroll='GreenAleph'
           AND b.WLCA IN ('Win','Loss','Cashout')
           AND l.LeagueName<>'NBA'""")
    other_rows = cur.fetchall()
    other = defaultdict(float)
    for r in other_rows:
        key = (r["LeagueName"], r["EventType"], r["EventLabel"])
        other[key] += float(r["NetProfit"] or 0)

    # ---------- BUILD DATAFRAME ----------
    rec = []
    for (et, el) in futures_table_map:
        rec.append(dict(
            LeagueName="NBA", EventType=et, EventLabel=el,
            ActiveDollarsAtStake=round(act_stake[(et, el)], 2),
            ActiveExpectedPayout=round(act_exp[(et, el)], 2),
            RealizedNetProfit=round(realized[(et, el)], 2),
            ExpectedValue=round(act_exp[(et, el)] - act_stake[(et, el)] + realized[(et, el)], 2),
        ))
    for (lg, et, el), net in other.items():
        rec.append(dict(
            LeagueName=lg, EventType=et, EventLabel=el,
            ActiveDollarsAtStake=0.0,
            ActiveExpectedPayout=0.0,
            RealizedNetProfit=round(net, 2),
            ExpectedValue=round(net, 2),
        ))
    df = pd.DataFrame(rec).sort_values(["LeagueName", "EventType", "EventLabel"]).reset_index(drop=True)

    # ---------- TOTAL ROW ----------
    cur.execute("SELECT SUM(NetProfit) AS total_net FROM bets WHERE WhichBankroll='GreenAleph'")
    total_net = float(cur.fetchone()["total_net"] or 0)

    total_row = {
        "LeagueName": "TOTAL", "EventType": "", "EventLabel": "",
        "ActiveDollarsAtStake": df["ActiveDollarsAtStake"].sum(),
        "ActiveExpectedPayout": df["ActiveExpectedPayout"].sum(),
        "RealizedNetProfit": round(total_net, 2),
        "ExpectedValue": round(
            df["ActiveExpectedPayout"].sum() - df["ActiveDollarsAtStake"].sum() + total_net, 2
        ),
    }
    df = pd.concat([df, pd.DataFrame([total_row])], ignore_index=True)

    cur.close()
    bet_cnx.close()
    fut_cnx.close()
    return df
# ╰──────────────────────────────────────────────────────────╯


# ╭──────────────────────  STREAMLIT UI  ────────────────────╮
st.set_page_config(page_title="NBA Futures EV", layout="wide")
st.title("NBA Futures – Expected Value Dashboard")

# Build table, snapshot EV, fetch history
df = build_ev_table()
total_ev = float(df.loc[df.LeagueName.eq("TOTAL"), "ExpectedValue"].iloc[0])
_save_snapshot(total_ev)
history = _load_history()

# Display current EV table
st.subheader("Current EV Table")
st.dataframe(
    df.style.format({
        "ActiveDollarsAtStake": "${:,.0f}",
        "ActiveExpectedPayout": "${:,.0f}",
        "RealizedNetProfit":   "${:,.0f}",
        "ExpectedValue":       "${:,.0f}",
    }),
    use_container_width=True,
)

# Display summary metrics
col1, col2, col3, col4 = st.columns(4)
metrics_df = df[df.LeagueName != "TOTAL"]
col1.metric("Active Stake",        f"${metrics_df['ActiveDollarsAtStake'].sum():,.0f}")
col2.metric("Expected Payout",     f"${metrics_df['ActiveExpectedPayout'].sum():,.0f}")
col3.metric("Realized Net Profit", f"${metrics_df['RealizedNetProfit'].sum():,.0f}")
col4.metric("Expected Value",      f"${metrics_df['ExpectedValue'].sum():,.0f}")

# Plot EV over time
st.subheader("Total Expected Value Over Time")
if not history.empty:
    st.line_chart(history.set_index("date")["ev"])
else:
    st.info("No history yet – snapshot will appear after today’s run.")
# ╰──────────────────────────────────────────────────────────╯
