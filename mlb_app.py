import streamlit as st
import pymysql
from collections import defaultdict
from datetime import datetime, timedelta, time
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import re

# ──────────────────────────────  CONFIG  ────────────────────────────────────
st.set_page_config(page_title="NBA Futures Dashboard", layout="wide")

# ──────────────────────────────  DB CONNECTIONS  ────────────────────────────
@st.cache_resource(show_spinner=False)
def get_betting_conn():
    return pymysql.connect(
        host      = st.secrets["BETTING_DB"]["host"],
        user      = st.secrets["BETTING_DB"]["user"],
        password  = st.secrets["BETTING_DB"]["password"],
        database  = st.secrets["BETTING_DB"]["database"],
        cursorclass = pymysql.cursors.DictCursor,
        autocommit = False
    )

@st.cache_resource(show_spinner=False)
def get_futures_conn():
    return pymysql.connect(
        host      = st.secrets["FUTURES_DB"]["host"],
        user      = st.secrets["FUTURES_DB"]["user"],
        password  = st.secrets["FUTURES_DB"]["password"],
        database  = st.secrets["FUTURES_DB"]["database"],
        cursorclass = pymysql.cursors.DictCursor,
        autocommit = False
    )

# ──────────────────────────────  ODDS HELPERS  ─────────────────────────────
def american_odds_to_decimal(odds: int) -> float:
    """+300 → 4.0, -150 → 1.666… , 0 → 1.0"""
    return 1.0 + (odds/100.0) if odds > 0 else 1.0 + (100.0 / abs(odds)) if odds != 0 else 1.0

def american_odds_to_probability(odds: int) -> float:
    """+200 → 0.333…, -150 → 0.60, 0 → 0.0"""
    return 100.0/(odds+100.0) if odds > 0 else abs(odds)/(abs(odds)+100.0) if odds != 0 else 0.0

def safe_cast_odds(val):
    if val in (None, "", 0):           return 0
    if isinstance(val, (int, float)):  return int(val)
    m = re.search(r"[-+]?\d+", str(val))
    return int(m.group()) if m else 0

# ──────────────────────────────  MAPPINGS  ──────────────────────────────────
futures_table_map = {
    ("Championship",           "NBA Championship") : "NBAChampionship",
    ("Conference Winner",      "Eastern Conference"): "NBAEasternConference",
    ("Conference Winner",      "Western Conference"): "NBAWesternConference",
    ("Defensive Player of Year Award", "Award")    : "NBADefensivePotY",
    ("Division Winner",        "Atlantic Division"): "NBAAtlantic",
    ("Division Winner",        "Central Division") : "NBACentral",
    ("Division Winner",        "Northwest Division"): "NBANorthwest",
    ("Division Winner",        "Pacific Division") : "NBAPacific",
    ("Division Winner",        "Southeast Division"): "NBASoutheast",
    ("Division Winner",        "Southwest Division"): "NBASouthwest",
    ("Most Improved Player Award", "Award")        : "NBAMIP",
    ("Most Valuable Player Award", "Award")        : "NBAMVP",
    ("Rookie of Year Award",   "Award")            : "NBARotY",
    ("Sixth Man of Year Award","Award")            : "NBASixthMotY",
}

#  short name fix‑ups
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

sportsbook_cols = ["BetMGM","DraftKings","Caesars","ESPNBet","FanDuel","BallyBet","RiversCasino","Bet365"]

# ───────────────────────  BEST ODDS (live or snapshot)  ──────────────────────
def query_best_american_odds(table:str, alias:str, date_cutoff:datetime, conn) -> int:
    """
    Returns the **largest non‑zero American odds** (across all books) for alias
    on or before date_cutoff. 0 if nothing found.
    """
    with conn.cursor() as cur:
        cur.execute(
            f"""
              SELECT {','.join(sportsbook_cols)}
                FROM {table}
               WHERE team_name = %s AND date_created <= %s
               ORDER BY date_created DESC
               LIMIT 1
            """,
            (alias, date_cutoff)
        )
        row = cur.fetchone()
    if not row: return 0
    nums = [safe_cast_odds(row[c]) for c in sportsbook_cols]
    nums = [n for n in nums if n != 0]
    return max(nums) if nums else 0

def get_best_decimal_prob(event_type:str, event_label:str, team:str,
                          ts:datetime, futures_conn):
    table = futures_table_map.get((event_type, event_label))
    if not table:
        return 1.0, 0.0
    alias = team_alias_map.get(team, team)
    am = query_best_american_odds(table, alias, ts, futures_conn)
    return (american_odds_to_decimal(am), american_odds_to_probability(am)) if am else (1.0,0.0)

# ──────────────────────────────  EV  TABLE  PAGE  ───────────────────────────
def render_ev_table():
    st.header("EV Table (by Market)")

    betting_conn  = get_betting_conn()
    futures_conn  = get_futures_conn()
    now_ts        = datetime.utcnow()

    # ---------- pull Active wagers ----------
    q_active = """
        SELECT b.WagerID, b.PotentialPayout, b.DollarsAtStake,
               l.EventType, l.EventLabel, l.ParticipantName
          FROM bets b
          JOIN legs l ON b.WagerID = l.WagerID
         WHERE b.WhichBankroll='GreenAleph'
           AND b.WLCA='Active'
           AND l.LeagueName='NBA'
    """
    with betting_conn.cursor() as cur:
        cur.execute(q_active)
        rows = cur.fetchall()

    # aggregate by wager
    bet_info = defaultdict(lambda: {"pot":0.0,"stake":0.0,"legs":[]})
    for r in rows:
        b = bet_info[r["WagerID"]]
        b["pot"]   = b["pot"]   or float(r["PotentialPayout"] or 0)
        b["stake"] = b["stake"] or float(r["DollarsAtStake"] or 0)
        b["legs"].append( (r["EventType"], r["EventLabel"], r["ParticipantName"]) )

    active_stake, active_exp = defaultdict(float), defaultdict(float)

    for data in bet_info.values():
        pot, stake, legs = data["pot"], data["stake"], data["legs"]
        probs, dec_list  = [], []
        for et,el,pn in legs:
            dec, prob = get_best_decimal_prob(et, el, pn, now_ts, futures_conn)
            dec_list.append( (dec,et,el) )
            probs.append(prob)
        if 0 in probs:          # at least one leg missing odds
            continue
        parlay_prob = 1.0
        for p in probs: parlay_prob *= p
        exp_pay     = pot * parlay_prob
        sum_excess  = sum(dec-1 for dec,_,_ in dec_list)
        if sum_excess <= 0:     # shouldn’t happen but guard
            continue
        for dec,et,el in dec_list:
            w = (et,el)
            frac = (dec-1)/sum_excess
            active_stake[w] += frac * stake
            active_exp  [w] += frac * exp_pay

    # ---------- pull realized W/L/C ----------
    realized_net = defaultdict(float)
    q_real = """
        SELECT b.WagerID, b.NetProfit,
               l.EventType, l.EventLabel, l.ParticipantName
          FROM bets b
          JOIN legs l ON b.WagerID=l.WagerID
         WHERE b.WhichBankroll='GreenAleph'
           AND b.WLCA IN ('Win','Loss','Cashout')
           AND l.LeagueName='NBA'
    """
    with betting_conn.cursor() as cur:
        cur.execute(q_real)
        rows = cur.fetchall()

    # collect legs per wager
    wager_legs = defaultdict(list)
    wager_net  = {}
    for r in rows:
        wager_legs[r["WagerID"]].append(
            (r["EventType"], r["EventLabel"], r["ParticipantName"])
        )
        wager_net[r["WagerID"]] = float(r["NetProfit"] or 0)

    for wid,legs in wager_legs.items():
        net = wager_net[wid]
        decs = [
            ( get_best_decimal_prob(et,el,pn,now_ts,futures_conn)[0] , et, el )
            for et,el,pn in legs
        ]
        sum_excess = sum(d-1 for d,_,_ in decs)
        if sum_excess <= 0: continue
        for d,et,el in decs:
            realized_net[(et,el)] += net * ((d-1)/sum_excess)

    # ---------- build dataframe ----------
    keys = set(active_stake)|set(active_exp)|set(realized_net)
    rows_out = []
    for et,el in sorted(keys):
        stake = active_stake.get((et,el),0)
        exp   = active_exp.get((et,el),0)
        net   = realized_net.get((et,el),0)
        ev    = exp - stake + net
        rows_out.append({
            "EventType": et,
            "EventLabel": el,
            "ActiveDollarsAtStake": round(stake,2),
            "ActiveExpectedPayout": round(exp,2),
            "RealizedNetProfit":    round(net,2),
            "ExpectedValue":        round(ev,2)
        })

    df = pd.DataFrame(rows_out)
    if not df.empty:
        df = df.sort_values(["EventType","EventLabel"]).reset_index(drop=True)
    st.dataframe(df, use_container_width=True)

# ──────────────────────────────  % RETURN  PAGE  ────────────────────────────
def render_return_plot():
    st.header("% Return Plot")

    # pick filters
    ev_types = sorted({k[0] for k in futures_table_map})
    event_type  = st.selectbox("Event Type", ev_types)
    labels      = sorted({label for (etype,label) in futures_table_map if etype==event_type})
    event_label = st.selectbox("Event Label", labels)

    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start",  datetime.utcnow().date()-timedelta(days=30))
    with col2:
        end_date   = st.date_input("End",    datetime.utcnow().date())

    if start_date > end_date:
        st.error("Start date must be before end date.")
        return

    if st.button("Generate Plot"):
        betting_conn = get_betting_conn()
        futures_conn = get_futures_conn()

        # pull Active wagers once
        with betting_conn.cursor() as cur:
            cur.execute("""
                SELECT b.WagerID, b.PotentialPayout, b.DollarsAtStake,
                       b.DateTimePlaced,
                       l.EventType, l.EventLabel, l.ParticipantName
                  FROM bets b
                  JOIN legs l ON b.WagerID=l.WagerID
                 WHERE b.WhichBankroll='GreenAleph'
                   AND b.WLCA='Active'
                   AND l.LeagueName='NBA'
            """)
            rows = cur.fetchall()

        # structure per wager
        wagers = defaultdict(lambda: {"pot":0,"stake":0,"placed":None,"legs":[]})
        for r in rows:
            w = wagers[r["WagerID"]]
            w["pot"]   = float(r["PotentialPayout"] or 0)
            w["stake"] = float(r["DollarsAtStake"]  or 0)
            placed_dt  = r["DateTimePlaced"]
            if isinstance(placed_dt, str):
                placed_dt = datetime.fromisoformat(placed_dt)
            w["placed"] = placed_dt
            w["legs"].append( (r["EventType"], r["EventLabel"], r["ParticipantName"]) )

        # time series
        date_range = pd.date_range(start_date, end_date, freq="D")
        series = []

        for d in date_range:
            ts = datetime.combine(d, time(hour=23,minute=59,second=59))
            tot_net = tot_stake = 0.0
            for w in wagers.values():
                if w["placed"] and w["placed"] > ts:   # bet not yet placed
                    continue

                # compute parlay probability using *current snapshot* odds <= ts
                parlay_prob = 1.0
                leg_dec = []
                for et,el,pn in w["legs"]:
                    dec, prob = get_best_decimal_prob(et, el, pn, ts, futures_conn)
                    if prob == 0:
                        parlay_prob = 0
                        break
                    leg_dec.append((dec, et, el))
                    parlay_prob *= prob
                if parlay_prob == 0:   # skip whole wager
                    continue

                net   = (w["pot"]*parlay_prob) - w["stake"]
                sum_excess = sum(dec-1 for dec,_,_ in leg_dec)
                if sum_excess <= 0: continue

                # allocate only legs that match filter
                for dec,et,el in leg_dec:
                    if (et,el) == (event_type, event_label):
                        frac = (dec-1)/sum_excess
                        tot_net   += frac * net
                        tot_stake += frac * w["stake"]

            ret_pct = (tot_net / tot_stake)*100 if tot_stake else 0.0
            series.append( (d, ret_pct) )

        # plot
        if not series:
            st.info("No data for the selected filters.")
            return
        dates, values = zip(*series)

        fig, ax = plt.subplots(figsize=(10,5))
        ax.plot(dates, values, marker='o')
        ax.set_title(f"% Return Over Time – {event_type}: {event_label}")
        ax.set_ylabel("Return (%)")
        ax.set_xlabel("Date")
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
        plt.xticks(rotation=45)
        st.pyplot(fig, use_container_width=True)

# ──────────────────────────────  ROUTER  ─────────────────────────────────────
page = st.sidebar.radio("Select Page", ["EV Table", "% Return Plot"])

if page == "EV Table":
    render_ev_table()
else:
    render_return_plot()
