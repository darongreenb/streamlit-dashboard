# ──────────────────────  NBA Futures Dashboard  ──────────────────────
import streamlit as st
import pymysql, re
from pymysql.err import OperationalError
import pandas as pd
import matplotlib.pyplot as plt, matplotlib.dates as mdates
from collections import defaultdict
from datetime import datetime, timedelta

# ───────────────────  PAGE CONFIG  ───────────────────
st.set_page_config(page_title="NBA Futures Dashboard", layout="wide")

# ───────────────────  CONNECTION HELPER  ─────────────
def safe_mysql_connect(**kwargs):
    """Open a MySQL connection or stop the app with a readable error."""
    try:
        return pymysql.connect(**kwargs,
                               autocommit=True,
                               cursorclass=pymysql.cursors.DictCursor)
    except OperationalError as e:
        st.error(f"❌  Could not connect to MySQL\n\n```{e}```")
        st.stop()

def new_betting_conn():
    return safe_mysql_connect(
        host     = st.secrets["BETTING_DB"]["host"],
        user     = st.secrets["BETTING_DB"]["user"],
        password = st.secrets["BETTING_DB"]["password"],
        database = st.secrets["BETTING_DB"]["database"],
        port     = 3306,
    )

def new_futures_conn():
    return safe_mysql_connect(
        host     = st.secrets["FUTURES_DB"]["host"],
        user     = st.secrets["FUTURES_DB"]["user"],
        password = st.secrets["FUTURES_DB"]["password"],
        database = st.secrets["FUTURES_DB"]["database"],
        port     = 3306,
    )

def with_cursor(conn):
    conn.ping(reconnect=True)
    return conn.cursor()

# ───────────────────  ODDS HELPERS  ─────────────────
def american_odds_to_decimal(o:int)->float: return 1 + (o/100) if o>0 else 1 + 100/abs(o) if o else 1
def american_odds_to_prob   (o:int)->float: return 100/(o+100) if o>0 else abs(o)/(abs(o)+100) if o else 0
def cast_odds(v):
    if v in (None,"",0): return 0
    if isinstance(v,(int,float)): return int(v)
    m=re.search(r"[-+]?\d+",str(v)); return int(m.group()) if m else 0

# ───────────────────  TABLE / TEAM MAPS  ────────────
futures_table_map = {
    ("Championship","NBA Championship"):      "NBAChampionship",
    ("Conference Winner","Eastern Conference"): "NBAEasternConference",
    ("Conference Winner","Western Conference"): "NBAWesternConference",
    ("Division Winner","Atlantic Division"):   "NBAAtlantic",
    ("Division Winner","Central Division"):    "NBACentral",
    ("Division Winner","Northwest Division"):  "NBANorthwest",
    ("Division Winner","Pacific Division"):    "NBAPacific",
    ("Division Winner","Southeast Division"):  "NBASoutheast",
    ("Division Winner","Southwest Division"):  "NBASouthwest",
    ("Most Valuable Player Award","Award"):    "NBAMVP",
    ("Most Improved Player Award","Award"):    "NBAMIP",
    ("Defensive Player of Year Award","Award"):"NBADefensivePotY",
    ("Rookie of Year Award","Award"):          "NBARotY",
    ("Sixth Man of Year Award","Award"):       "NBASixthMotY",
}

# locations → nickname
team_alias_map = {
    "Milwaukee Bucks":"Bucks","Miami Heat":"Heat","Boston Celtics":"Celtics",
    "Philadelphia 76ers":"76ers","New York Knicks":"Knicks","Brooklyn Nets":"Nets",
    "Cleveland Cavaliers":"Cavaliers","Chicago Bulls":"Bulls","Detroit Pistons":"Pistons",
    "Toronto Raptors":"Raptors","Atlanta Hawks":"Hawks","Charlotte Hornets":"Hornets",
    "Orlando Magic":"Magic","Indiana Pacers":"Pacers","Washington Wizards":"Wizards",
    "Denver Nuggets":"Nuggets","Minnesota Timberwolves":"Timberwolves","Oklahoma City Thunder":"Thunder",
    "Utah Jazz":"Jazz","Portland Trail Blazers":"Trail Blazers","Golden State Warriors":"Warriors",
    "Los Angeles Clippers":"Clippers","Los Angeles Lakers":"Lakers","Phoenix Suns":"Suns",
    "Sacramento Kings":"Kings","Dallas Mavericks":"Mavericks","Houston Rockets":"Rockets",
    "San Antonio Spurs":"Spurs","Memphis Grizzlies":"Grizzlies","New Orleans Pelicans":"Pelicans",
}

sportsbook_cols = ["BetMGM","DraftKings","Caesars","ESPNBet","FanDuel",
                   "BallyBet","RiversCasino","Bet365"]

def best_odds_decimal_prob(event_type,event_label,participant,cutoff_dt,fut_conn):
    tbl = futures_table_map.get((event_type,event_label))
    if not tbl: return 1.0,0.0
    alias = team_alias_map.get(participant, participant)   # map "Miami Heat" → "Heat"
    with with_cursor(fut_conn) as cur:
        cur.execute(
            f"""SELECT {','.join(sportsbook_cols)}
                  FROM {tbl}
                 WHERE team_name=%s AND date_created<=%s
                 ORDER BY date_created DESC LIMIT 1""",
            (alias, cutoff_dt)
        )
        row = cur.fetchone()
    if not row: return 1.0,0.0
    nums=[cast_odds(row[c]) for c in sportsbook_cols if cast_odds(row[c])]
    if not nums: return 1.0,0.0
    best=max(nums)
    return american_odds_to_decimal(best), american_odds_to_prob(best)

# ───────────────────  EV TABLE PAGE  ──────────────
def ev_table_page():
    st.header("EV Table")
    bet_conn, fut_conn = new_betting_conn(), new_futures_conn()
    now = datetime.utcnow()

    # --- load active bets ----------------------------------------------------
    q_active = """
        SELECT b.WagerID,b.PotentialPayout,b.DollarsAtStake,
               l.EventType,l.EventLabel,l.ParticipantName
          FROM bets b JOIN legs l ON b.WagerID=l.WagerID
         WHERE b.WhichBankroll='GreenAleph' AND b.WLCA='Active' AND l.LeagueName='NBA'"""
    with with_cursor(bet_conn) as cur:
        cur.execute(q_active)
        rows = cur.fetchall()

    active = defaultdict(lambda:{"pot":0,"stake":0,"legs":[]})
    for r in rows:
        a=active[r["WagerID"]]
        a["pot"]=a["pot"] or float(r["PotentialPayout"] or 0)
        a["stake"]=a["stake"] or float(r["DollarsAtStake"] or 0)
        a["legs"].append((r["EventType"],r["EventLabel"],r["ParticipantName"]))

    stake_by,exp_by=defaultdict(float),defaultdict(float)
    for rec in active.values():
        pot,stake,legs=rec["pot"],rec["stake"],rec["legs"]
        prob=1.0; decs=[]
        for et,el,pn in legs:
            dec,p=best_odds_decimal_prob(et,el,pn,now,fut_conn)
            if p==0: prob=0; break
            decs.append((dec,et,el)); prob*=p
        if prob==0: continue
        expected=pot*prob
        s_exc=sum(d-1 for d,_,_ in decs)
        if s_exc<=0: continue
        for d,et,el in decs:
            w=(d-1)/s_exc
            stake_by[(et,el)]+=w*stake
            exp_by  [(et,el)]+=w*expected

    # --- realised ------------------------------------------------------------
    q_real = """
        SELECT b.WagerID,b.NetProfit,
               l.EventType,l.EventLabel,l.ParticipantName
          FROM bets b JOIN legs l ON b.WagerID=l.WagerID
         WHERE b.WhichBankroll='GreenAleph'
           AND b.WLCA IN ('Win','Loss','Cashout') AND l.LeagueName='NBA'"""
    with with_cursor(bet_conn) as cur:
        cur.execute(q_real)
        rows=cur.fetchall()

    net_by           = defaultdict(float)
    legs_per_wager   = defaultdict(list)
    for r in rows:
        net_by[r["WagerID"]]=float(r["NetProfit"] or 0)
        legs_per_wager[r["WagerID"]].append((r["EventType"],r["EventLabel"],r["ParticipantName"]))

    realized_by = defaultdict(float)
    for wid,legs in legs_per_wager.items():
        decs=[(best_odds_decimal_prob(et,el,pn,now,fut_conn)[0],et,el) for et,el,pn in legs]
        s_exc=sum(d-1 for d,_,_ in decs)
        if s_exc<=0: continue
        for d,et,el in decs:
            realized_by[(et,el)] += net_by[wid]*((d-1)/s_exc)

    bet_conn.close(); fut_conn.close()

    # --- dataframe -----------------------------------------------------------
    keys=set(stake_by)|set(exp_by)|set(realized_by)
    rows=[]
    for et,el in sorted(keys):
        stake=stake_by.get((et,el),0); exp=exp_by.get((et,el),0); net=realized_by.get((et,el),0)
        rows.append(dict(EventType=et,EventLabel=el,
                         ActiveDollarsAtStake=round(stake,2),
                         ActiveExpectedPayout=round(exp,2),
                         RealizedNetProfit=round(net,2),
                         ExpectedValue=round(exp-stake+net,2)))
    st.dataframe(pd.DataFrame(rows)
                 .sort_values(["EventType","EventLabel"])
                 .reset_index(drop=True),
                 use_container_width=True)

# ───────────────────  % RETURN PLOT PAGE  ────────
def return_plot_page_fast():
    st.header("% Return Plot")

    fut_conn, bet_conn = new_futures_conn(), new_betting_conn()

    sel_type = st.selectbox("Event Type", sorted({t for t,_ in futures_table_map}))
    sel_lbl  = st.selectbox("Event Label", sorted({l for t,l in futures_table_map if t==sel_type}))
    col1,col2=st.columns(2)
    start_d  = col1.date_input("Start", datetime.utcnow().date()-timedelta(days=60))
    end_d    = col2.date_input("End",   datetime.utcnow().date())
    if start_d>end_d: st.error("Start must precede End"); return
    if not st.button("Generate Plot"): st.stop()

    # ---- wagers -------------------------------------------------------------
    with with_cursor(bet_conn) as cur:
        cur.execute("""
            SELECT b.WagerID,b.PotentialPayout,b.DollarsAtStake,b.DateTimePlaced,
                   l.EventType,l.EventLabel,l.ParticipantName
              FROM bets b JOIN legs l ON b.WagerID=l.WagerID
             WHERE b.WhichBankroll='GreenAleph' AND b.WLCA='Active' AND l.LeagueName='NBA'""")
        bets = pd.DataFrame(cur.fetchall())
    if bets.empty: st.warning("No active wagers"); return

    # weight = equal share of stake among legs in target market for each wager
    wt = (bets.assign(target=lambda d:(d["EventType"]==sel_type)&(d["EventLabel"]==sel_lbl))
                .groupby("WagerID")["target"]
                .apply(lambda s: 1/s.sum() if s.any() else 0)
                .rename("Weight").reset_index())
    bets = bets.merge(wt, on="WagerID").query("Weight>0")
    if bets.empty: st.warning("No legs in that market"); return
    st.info(f"{bets['WagerID'].nunique()} active wagers include that market.")

    # ---- bulk odds ----------------------------------------------------------
    names = bets["ParticipantName"].map(lambda n: team_alias_map.get(n,n)).unique().tolist()
    tbl   = futures_table_map[(sel_type,sel_lbl)]

    @st.cache_data(ttl=3600,show_spinner=False)
    def pull_odds(tbl, names, start,end):
        if not names: return pd.DataFrame()
        ph=",".join(["%s"]*len(names))
        with with_cursor(fut_conn) as cur:
            cur.execute(
                f"""SELECT team_name,date_created,{','.join(sportsbook_cols)}
                      FROM {tbl}
                     WHERE team_name IN ({ph})
                       AND date_created BETWEEN %s AND %s
                     ORDER BY team_name,date_created""",
                (*names,f"{start} 00:00:00",f"{end} 23:59:59"))
            raw=pd.DataFrame(cur.fetchall())
        if raw.empty: return raw
        raw[sportsbook_cols]=raw[sportsbook_cols].apply(pd.to_numeric,errors="coerce").fillna(0)
        raw["best"]=raw[sportsbook_cols].replace(0,pd.NA).max(axis=1).fillna(0).astype(int)
        raw["prob"]=raw["best"].apply(american_odds_to_prob)
        raw["date"]=pd.to_datetime(raw["date_created"]).dt.date
        return (raw.sort_values(["team_name","date"])
                   .groupby(["team_name","date"]).tail(1))[["team_name","date","prob"]]

    odds=pull_odds(tbl,names,start_d,end_d)
    if odds.empty: st.warning("No odds snapshots"); return

    # ---- assemble daily pnl --------------------------------------------------
    merged=(bets.merge(odds, left_on=bets["ParticipantName"].map(lambda n:team_alias_map.get(n,n)),
                               right_on="team_name")
                  .drop(columns="key_0"))
    merged["date"]=pd.to_datetime(merged["date"])
    merged["stake_part"]=merged["DollarsAtStake"]*merged["Weight"]
    merged["exp_part"]=merged["PotentialPayout"]*merged["prob"]*merged["Weight"]
    merged["net_part"]=merged["exp_part"]-merged["stake_part"]

    daily=(merged.groupby("date")
                 .agg(net=("net_part","sum"), stake=("stake_part","sum"))
                 .reset_index())
    daily["pct"]=daily["net"]/daily["stake"]*100
    daily=daily[(daily["date"].dt.date>=start_d)&(daily["date"].dt.date<=end_d)]
    if daily.empty: st.warning("Insufficient data"); return

    # ---- plot ---------------------------------------------------------------
    fig,ax=plt.subplots(figsize=(10,5))
    ax.plot(daily["date"],daily["pct"],marker="o")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    ax.set_title(f"% Return — {sel_type}: {sel_lbl}")
    ax.set_ylabel("Return (%)"); ax.set_xlabel("Date")
    plt.xticks(rotation=45)
    st.pyplot(fig,use_container_width=True)

# ───────────────────  NAV  ───────────────────────
page=st.sidebar.radio("Page",["EV Table","% Return Plot"])
if page=="EV Table": ev_table_page()
else:               return_plot_page_fast()
