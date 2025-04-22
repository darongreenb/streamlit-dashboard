import streamlit as st
import pymysql, re
from collections import defaultdict
from datetime import datetime, timedelta, time
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# ─────────────────────────────  PAGE CONFIG  ─────────────────────────────
st.set_page_config(page_title="NBA Futures Dashboard", layout="wide")

# ─────────────────────────────  DB HELPERS  ──────────────────────────────
def new_betting_conn():
    return pymysql.connect(
        host     = st.secrets["BETTING_DB"]["host"],
        user     = st.secrets["BETTING_DB"]["user"],
        password = st.secrets["BETTING_DB"]["password"],
        database = st.secrets["BETTING_DB"]["database"],
        cursorclass = pymysql.cursors.DictCursor ,
        autocommit  = True
    )

def new_futures_conn():
    return pymysql.connect(
        host     = st.secrets["FUTURES_DB"]["host"],
        user     = st.secrets["FUTURES_DB"]["user"],
        password = st.secrets["FUTURES_DB"]["password"],
        database = st.secrets["FUTURES_DB"]["database"],
        cursorclass = pymysql.cursors.DictCursor ,
        autocommit  = True
    )

def with_cursor(conn):
    """Context‑manager that pings first, guarantees cursor close."""
    conn.ping(reconnect=True)
    return conn.cursor()

# ─────────────────────────────  ODDS HELPERS  ────────────────────────────
def american_odds_to_decimal(o:int)->float: return 1.0 + (o/100) if o>0 else 1.0 + 100/abs(o) if o else 1.0
def american_odds_to_prob(o:int)->float:    return 100/(o+100) if o>0 else abs(o)/(abs(o)+100) if o else 0.0
def cast_odds(v): 
    if v in (None,"",0): return 0
    if isinstance(v,(int,float)): return int(v)
    m=re.search(r"[-+]?\d+",str(v)); return int(m.group()) if m else 0

# ─────────────────────────────  MAPPINGS  ───────────────────────────────
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
    "Memphis Grizzlies":"Grizzlies","Atlanta Hawks":"Hawks","Miami Heat":"Heat","Charlotte Hornets":"Hornets",
    "Utah Jazz":"Jazz","Sacramento Kings":"Kings","New York Knicks":"Knicks","Los Angeles Lakers":"Lakers",
    "Orlando Magic":"Magic","Dallas Mavericks":"Mavericks","Brooklyn Nets":"Nets","Denver Nuggets":"Nuggets",
    "Indiana Pacers":"Pacers","New Orleans Pelicans":"Pelicans","Detroit Pistons":"Pistons",
    "Toronto Raptors":"Raptors","Houston Rockets":"Rockets","San Antonio Spurs":"Spurs","Phoenix Suns":"Suns",
    "Oklahoma City Thunder":"Thunder","Minnesota Timberwolves":"Timberwolves","Portland Trail Blazers":"Trail Blazers",
    "Golden State Warriors":"Warriors","Washington Wizards":"Wizards",
}
sportsbook_cols = ["BetMGM","DraftKings","Caesars","ESPNBet","FanDuel","BallyBet","RiversCasino","Bet365"]

def best_odds_decimal_prob(event_type,event_label,participant,cutoff_dt,fut_conn):
    tbl=futures_table_map.get((event_type,event_label))
    if not tbl: return 1.0,0.0
    alias=team_alias_map.get(participant,participant)
    with with_cursor(fut_conn) as cur:
        cur.execute(
            f"""SELECT {','.join(sportsbook_cols)} 
                  FROM {tbl} 
                 WHERE team_name=%s AND date_created<=%s
                 ORDER BY date_created DESC LIMIT 1""",
            (alias, cutoff_dt)
        )
        row=cur.fetchone()
    if not row: return 1.0,0.0
    nums=[cast_odds(row[c]) for c in sportsbook_cols]; nums=[n for n in nums if n]
    if not nums: return 1.0,0.0
    best=max(nums)
    return american_odds_to_decimal(best), american_odds_to_prob(best)

# ─────────────────────────────  EV TABLE  ───────────────────────────────
def ev_table_page():
    st.header("EV Table")

    bet_conn = new_betting_conn()
    fut_conn = new_futures_conn()
    now      = datetime.utcnow()

    # -------- active wagers -------
    sql_active = """
        SELECT b.WagerID,b.PotentialPayout,b.DollarsAtStake,
               l.EventType,l.EventLabel,l.ParticipantName
          FROM bets b JOIN legs l ON b.WagerID=l.WagerID
         WHERE b.WhichBankroll='GreenAleph' AND b.WLCA='Active' AND l.LeagueName='NBA'
    """
    with with_cursor(bet_conn) as cur:
        cur.execute(sql_active)
        rows=cur.fetchall()

    active_bets=defaultdict(lambda:{"pot":0,"stake":0,"legs":[]})
    for r in rows:
        w=active_bets[r["WagerID"]]
        w["pot"]=w["pot"] or float(r["PotentialPayout"] or 0)
        w["stake"]=w["stake"] or float(r["DollarsAtStake"] or 0)
        w["legs"].append((r["EventType"],r["EventLabel"],r["ParticipantName"]))

    active_stake,active_exp=defaultdict(float),defaultdict(float)
    for data in active_bets.values():
        pot,stake,legs=data["pot"],data["stake"],data["legs"]
        decs=[]; prob=1.0
        for et,el,pn in legs:
            dec,p=best_odds_decimal_prob(et,el,pn,now,fut_conn)
            if p==0: prob=0; break
            decs.append((dec,et,el)); prob*=p
        if prob==0: continue
        expected=pot*prob
        sum_exc=sum(d-1 for d,_,_ in decs);      # proportional weights
        if sum_exc<=0: continue
        for d,et,el in decs:
            w=(d-1)/sum_exc
            active_stake[(et,el)]+=w*stake
            active_exp  [(et,el)]+=w*expected

    # -------- realized net profit -------
    sql_real = """
        SELECT b.WagerID,b.NetProfit,
               l.EventType,l.EventLabel,l.ParticipantName
          FROM bets b JOIN legs l ON b.WagerID=l.WagerID
         WHERE b.WhichBankroll='GreenAleph'
           AND b.WLCA IN ('Win','Loss','Cashout')
           AND l.LeagueName='NBA'
    """
    with with_cursor(bet_conn) as cur:
        cur.execute(sql_real)
        rows=cur.fetchall()

    wager_net=defaultdict(float); wager_legs=defaultdict(list)
    for r in rows:
        wager_net[r["WagerID"]]=float(r["NetProfit"] or 0)
        wager_legs[r["WagerID"]].append((r["EventType"],r["EventLabel"],r["ParticipantName"]))

    realized_np=defaultdict(float)
    for wid,legs in wager_legs.items():
        net=wager_net[wid]
        decs=[(best_odds_decimal_prob(et,el,pn,now,fut_conn)[0],et,el) for et,el,pn in legs]
        sum_exc=sum(d-1 for d,_,_ in decs)
        if sum_exc<=0: continue
        for d,et,el in decs:
            realized_np[(et,el)]+=net*((d-1)/sum_exc)

    bet_conn.close(); fut_conn.close()

    # -------- table -------
    keys=set(active_stake)|set(active_exp)|set(realized_np)
    out=[]
    for et,el in sorted(keys):
        stake=active_stake.get((et,el),0); exp=active_exp.get((et,el),0); net=realized_np.get((et,el),0)
        out.append(dict(EventType=et,EventLabel=el,
                        ActiveDollarsAtStake=round(stake,2),
                        ActiveExpectedPayout=round(exp,2),
                        RealizedNetProfit=round(net,2),
                        ExpectedValue=round(exp-stake+net,2)))
    df=pd.DataFrame(out).sort_values(["EventType","EventLabel"]).reset_index(drop=True)
    st.dataframe(df,use_container_width=True)

# ────────────────────────  % RETURN PLOT  ─────────────────────────
def return_plot_page():
    st.header("% Return Plot")

    fut_conn=new_futures_conn(); bet_conn=new_betting_conn()

    ev_types=sorted({k[0] for k in futures_table_map})
    sel_type = st.selectbox("Event Type",ev_types)
    labels   = sorted({lbl for (et,lbl) in futures_table_map if et==sel_type})
    sel_lbl  = st.selectbox("Event Label",labels)

    col1,col2=st.columns(2)
    start_date=col1.date_input("Start",datetime.utcnow().date()-timedelta(days=30))
    end_date  =col2.date_input("End"  ,datetime.utcnow().date())
    if start_date>end_date:
        st.error("Start date must be <= end date"); return

    if not st.button("Generate Plot"):
        st.info("Adjust filters and click **Generate Plot**."); return

    # Load all ACTIVE wagers once
    with with_cursor(bet_conn) as cur:
        cur.execute("""
            SELECT b.WagerID,b.PotentialPayout,b.DollarsAtStake,b.DateTimePlaced,
                   l.EventType,l.EventLabel,l.ParticipantName
              FROM bets b JOIN legs l ON b.WagerID=l.WagerID
             WHERE b.WhichBankroll='GreenAleph'
               AND b.WLCA='Active' AND l.LeagueName='NBA'
        """)
        rows=cur.fetchall()

    wagers=defaultdict(lambda:{"pot":0,"stake":0,"placed":None,"legs":[]})
    for r in rows:
        w=wagers[r["WagerID"]]
        w["pot"]=float(r["PotentialPayout"] or 0)
        w["stake"]=float(r["DollarsAtStake"] or 0)
        w["placed"]=r["DateTimePlaced"]
        w["legs"].append((r["EventType"],r["EventLabel"],r["ParticipantName"]))

    dts=pd.date_range(start_date,end_date,freq="D")
    points=[]
    for d in dts:
        ts=datetime.combine(d,time.max)
        net_tot=stake_tot=0.0
        for w in wagers.values():
            if w["placed"] and w["placed"]>ts: continue
            parlay_prob=1.0; leg_info=[]
            for et,el,pn in w["legs"]:
                dec,p=best_odds_decimal_prob(et,el,pn,ts,fut_conn)
                if p==0: parlay_prob=0; break
                leg_info.append((dec,et,el)); parlay_prob*=p
            if parlay_prob==0: continue
            net=(w["pot"]*parlay_prob)-w["stake"]
            sum_exc=sum(d-1 for d,_,_ in leg_info)
            if sum_exc<=0: continue
            for dec,et,el in leg_info:
                if (et,el)==(sel_type,sel_lbl):
                    wgt=(dec-1)/sum_exc
                    net_tot  += wgt*net
                    stake_tot+= wgt*w["stake"]
        pct= (net_tot/stake_tot)*100 if stake_tot else 0.0
        points.append((d,pct))

    bet_conn.close(); fut_conn.close()

    if not points:
        st.info("No data for these filters."); return

    xs,ys=zip(*points)
    fig,ax=plt.subplots(figsize=(10,5))
    ax.plot(xs,ys,marker='o')
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
    ax.set_xlabel("Date"); ax.set_ylabel("Return (%)")
    ax.set_title(f"% Return – {sel_type}: {sel_lbl}")
    plt.xticks(rotation=45)
    st.pyplot(fig,use_container_width=True)

# ───────────────────────────  SIDEBAR NAV  ─────────────────────────
page=st.sidebar.radio("Choose Page",["EV Table","% Return Plot"])
if page=="EV Table": ev_table_page()
else:                return_plot_page()
