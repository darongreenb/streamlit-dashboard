import streamlit as st
import pymysql
from collections import defaultdict
from datetime import datetime, timedelta, time
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import re

# ─────────────────────────  PAGE CONFIG  ──────────────────────────
st.set_page_config(page_title="NBA Futures Dashboard", layout="wide")

# ────────────────────────  DB CONNECTIONS  ────────────────────────
@st.cache_resource(show_spinner=False)
def get_betting_conn():
    return pymysql.connect(
        host=st.secrets["BETTING_DB"]["host"],
        user=st.secrets["BETTING_DB"]["user"],
        password=st.secrets["BETTING_DB"]["password"],
        database=st.secrets["BETTING_DB"]["database"],
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False
    )

@st.cache_resource(show_spinner=False)
def get_futures_conn():
    return pymysql.connect(
        host=st.secrets["FUTURES_DB"]["host"],
        user=st.secrets["FUTURES_DB"]["user"],
        password=st.secrets["FUTURES_DB"]["password"],
        database=st.secrets["FUTURES_DB"]["database"],
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False
    )

# ─────────────────────────────  HELPERS  ──────────────────────────
def american_odds_to_decimal(odds:int) -> float:
    return 1.0 + (odds/100.0) if odds > 0 else 1.0 + (100.0/abs(odds)) if odds != 0 else 1.0

def american_odds_to_probability(odds:int) -> float:
    return 100.0/(odds+100.0) if odds > 0 else abs(odds)/(abs(odds)+100.0) if odds != 0 else 0.0

def safe_cast_odds(val):
    if val in (None,"",0): return 0
    if isinstance(val,(int,float)): return int(val)
    m = re.search(r"[-+]?\d+", str(val))
    return int(m.group()) if m else 0

# --- mappings (same as you provided) ---
futures_table_map = {
    ("Championship","NBA Championship"): "NBAChampionship",
    ("Conference Winner","Eastern Conference"): "NBAEasternConference",
    ("Conference Winner","Western Conference"): "NBAWesternConference",
    ("Defensive Player of Year Award","Award"): "NBADefensivePotY",
    ("Division Winner","Atlantic Division"): "NBAAtlantic",
    ("Division Winner","Central Division"): "NBACentral",
    ("Division Winner","Northwest Division"): "NBANorthwest",
    ("Division Winner","Pacific Division"): "NBAPacific",
    ("Division Winner","Southeast Division"): "NBASoutheast",
    ("Division Winner","Southwest Division"): "NBASouthwest",
    ("Most Improved Player Award","Award"): "NBAMIP",
    ("Most Valuable Player Award","Award"): "NBAMVP",
    ("Rookie of Year Award","Award"): "NBARotY",
    ("Sixth Man of Year Award","Award"): "NBASixthMotY",
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

def _best_american_odds(table:str, alias:str, cutoff:datetime, conn) -> int:
    with conn.cursor() as cur:
        cur.execute(
            f"""
              SELECT {','.join(sportsbook_cols)}
                FROM {table}
               WHERE team_name=%s AND date_created<=%s
               ORDER BY date_created DESC
               LIMIT 1
            """,
            (alias, cutoff)
        )
        row = cur.fetchone()
    if not row: return 0
    nums = [safe_cast_odds(row[c]) for c in sportsbook_cols]
    nums = [n for n in nums if n != 0]
    return max(nums) if nums else 0

def get_best_decimal_prob(event_type, event_label, participant, cutoff_ts, futures_conn):
    table = futures_table_map.get((event_type,event_label))
    if not table: return 1.0,0.0
    alias = team_alias_map.get(participant, participant)
    am = _best_american_odds(table, alias, cutoff_ts, futures_conn)
    return (american_odds_to_decimal(am), american_odds_to_probability(am)) if am else (1.0,0.0)

# ─────────────────────────  EV TABLE PAGE  ─────────────────────────
def ev_table_page():
    st.title("NBA Futures: Active & Realized Payouts by Market")

    bet_conn = get_betting_conn()
    fut_conn = get_futures_conn()
    now_ts   = datetime.utcnow()

    # ----- ACTIVE wagers -----
    q_active = """
        SELECT b.WagerID, b.PotentialPayout, b.DollarsAtStake,
               l.EventType, l.EventLabel, l.ParticipantName
          FROM bets b
          JOIN legs l ON b.WagerID=l.WagerID
         WHERE b.WhichBankroll='GreenAleph'
           AND b.WLCA='Active'
           AND l.LeagueName='NBA'
    """
    with bet_conn.cursor() as cur:
        cur.execute(q_active)
        rows = cur.fetchall()

    active_bets = defaultdict(lambda: {"pot":0,"stake":0,"legs":[]})
    for r in rows:
        w = active_bets[r["WagerID"]]
        w["pot"]   = w["pot"]   or float(r["PotentialPayout"] or 0)
        w["stake"] = w["stake"] or float(r["DollarsAtStake"]  or 0)
        w["legs"].append((r["EventType"],r["EventLabel"],r["ParticipantName"]))

    active_stake, active_exp = defaultdict(float), defaultdict(float)
    for data in active_bets.values():
        pot, stake, legs = data["pot"], data["stake"], data["legs"]
        probs, decs = [],[]
        for et,el,pn in legs:
            dec,prob = get_best_decimal_prob(et,el,pn,now_ts,fut_conn)
            probs.append(prob); decs.append((dec,et,el))
        if 0 in probs: continue
        parlay_prob = 1.0
        for p in probs: parlay_prob*=p
        expected = pot*parlay_prob
        sum_exc  = sum(d-1 for d,_,_ in decs)
        if sum_exc<=0: continue
        for d,et,el in decs:
            frac=(d-1)/sum_exc
            active_stake[(et,el)] += frac*stake
            active_exp  [(et,el)] += frac*expected

    # ----- realized NetProfit -----
    realized_np = defaultdict(float)
    q_real = """
        SELECT b.WagerID, b.NetProfit,
               l.EventType,l.EventLabel,l.ParticipantName
          FROM bets b
          JOIN legs l ON b.WagerID=l.WagerID
         WHERE b.WhichBankroll='GreenAleph'
           AND b.WLCA IN ('Win','Loss','Cashout')
           AND l.LeagueName='NBA'
    """
    with bet_conn.cursor() as cur:
        cur.execute(q_real)
        rows = cur.fetchall()

    wager_net = defaultdict(float)
    wager_legs= defaultdict(list)
    for r in rows:
        wager_net[r["WagerID"]] = float(r["NetProfit"] or 0)
        wager_legs[r["WagerID"]].append((r["EventType"],r["EventLabel"],r["ParticipantName"]))

    for wid,legs in wager_legs.items():
        net = wager_net[wid]
        decs = [(get_best_decimal_prob(et,el,pn,now_ts,fut_conn)[0],et,el) for et,el,pn in legs]
        sum_exc = sum(d-1 for d,_,_ in decs)
        if sum_exc<=0: continue
        for d,et,el in decs:
            realized_np[(et,el)] += net*((d-1)/sum_exc)

    # ----- assemble table -----
    keys = set(active_stake)|set(active_exp)|set(realized_np)
    records=[]
    for et,el in sorted(keys):
        stake = active_stake.get((et,el),0)
        exp   = active_exp.get((et,el),0)
        net   = realized_np.get((et,el),0)
        ev    = exp - stake + net
        records.append({
            "EventType":et,"EventLabel":el,
            "ActiveDollarsAtStake":round(stake,2),
            "ActiveExpectedPayout":round(exp,2),
            "RealizedNetProfit":round(net,2),
            "ExpectedValue":round(ev,2)
        })
    df = pd.DataFrame(records).sort_values(["EventType","EventLabel"]).reset_index(drop=True)
    st.dataframe(df,use_container_width=True)

# ────────────────────────  % RETURN PLOT PAGE  ─────────────────────
def return_plot_page():
    st.title("% Return Plot")

    fut_conn = get_futures_conn()
    bet_conn = get_betting_conn()

    ev_types = sorted({k[0] for k in futures_table_map})
    sel_type  = st.selectbox("Event Type", ev_types)
    labels    = sorted({lbl for (et,lbl) in futures_table_map if et==sel_type})
    sel_label = st.selectbox("Event Label", labels)

    col1,col2=st.columns(2)
    with col1:
        start_d = st.date_input("Start", datetime.utcnow().date()-timedelta(days=30))
    with col2:
        end_d   = st.date_input("End",   datetime.utcnow().date())

    if start_d> end_d:
        st.error("Start date must be before end date."); return

    if not st.button("Generate Plot"):
        st.info("Choose filters & press **Generate Plot**")
        return

    # ---- load Active bets once ----
    with bet_conn.cursor() as cur:
        cur.execute("""
            SELECT b.WagerID,b.PotentialPayout,b.DollarsAtStake,b.DateTimePlaced,
                   l.EventType,l.EventLabel,l.ParticipantName
              FROM bets b
              JOIN legs l ON b.WagerID=l.WagerID
             WHERE b.WhichBankroll='GreenAleph'
               AND b.WLCA='Active'
               AND l.LeagueName='NBA'
        """)
        rows = cur.fetchall()

    wagers = defaultdict(lambda: {"pot":0,"stake":0,"placed":None,"legs":[]})
    for r in rows:
        w=wagers[r["WagerID"]]
        w["pot"]=float(r["PotentialPayout"] or 0)
        w["stake"]=float(r["DollarsAtStake"] or 0)
        ts=r["DateTimePlaced"];                    # pymysql already returns datetime
        w["placed"]=ts
        w["legs"].append((r["EventType"],r["EventLabel"],r["ParticipantName"]))

    dates= pd.date_range(start_d,end_d,freq="D")
    series=[]
    for d in dates:
        ts=datetime.combine(d,time(23,59,59))
        net_tot=stake_tot=0.0
        for w in wagers.values():
            if w["placed"] and w["placed"]>ts: continue
            parlay_prob=1.0
            leg_dec=[]
            for et,el,pn in w["legs"]:
                dec,prob = get_best_decimal_prob(et,el,pn,ts,fut_conn)
                if prob==0: parlay_prob=0; break
                leg_dec.append((dec,et,el)); parlay_prob*=prob
            if parlay_prob==0: continue
            net=(w["pot"]*parlay_prob)-w["stake"]
            sum_exc=sum(dec-1 for dec,_,_ in leg_dec)
            if sum_exc<=0: continue
            for dec,et,el in leg_dec:
                if (et,el)==(sel_type,sel_label):
                    frac=(dec-1)/sum_exc
                    net_tot  += frac*net
                    stake_tot+= frac*w["stake"]
        pct=(net_tot/stake_tot)*100 if stake_tot else 0.0
        series.append((d,pct))

    if not series:
        st.info("No data for the chosen filters."); return

    xs,ys=zip(*series)
    fig,ax=plt.subplots(figsize=(10,5))
    ax.plot(xs,ys,marker='o')
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
    ax.set_ylabel("Return (%)"); ax.set_xlabel("Date")
    ax.set_title(f"% Return — {sel_type}: {sel_label}")
    plt.xticks(rotation=45)
    st.pyplot(fig,use_container_width=True)

# ────────────────────────────  NAVIGATION  ────────────────────────
page = st.sidebar.radio("Select Page", ["EV Table", "% Return Plot"])

if page == "EV Table":
    ev_table_page()
else:
    return_plot_page()
