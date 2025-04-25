# ─────────────────────  NBA Futures Dashboard  ──────────────────────
import streamlit as st
import pymysql, re
from collections import defaultdict
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# ─────────────────  PAGE CONFIG  ─────────────────
st.set_page_config(page_title="NBA Futures Dashboard", layout="wide")

# ─────────────────  DB HELPERS  ──────────────────
def new_betting_conn():
    return pymysql.connect(
        host       = st.secrets["BETTING_DB"]["host"],
        user       = st.secrets["BETTING_DB"]["user"],
        password   = st.secrets["BETTING_DB"]["password"],
        database   = st.secrets["BETTING_DB"]["database"],
        cursorclass= pymysql.cursors.DictCursor,
        autocommit = True,
    )

def new_futures_conn():
    return pymysql.connect(
        host       = st.secrets["FUTURES_DB"]["host"],
        user       = st.secrets["FUTURES_DB"]["user"],
        password   = st.secrets["FUTURES_DB"]["password"],
        database   = st.secrets["FUTURES_DB"]["database"],
        cursorclass= pymysql.cursors.DictCursor,
        autocommit = True,
    )

def with_cursor(conn):
    """Ping & yield a cursor that always closes."""
    conn.ping(reconnect=True)
    return conn.cursor()

# ─────────────────  ODDS / CAST HELPERS  ─────────
def american_odds_to_decimal(o:int)->float: return 1 + (o/100) if o>0 else 1 + 100/abs(o) if o else 1
def american_odds_to_prob(o:int)->float:    return 100/(o+100) if o>0 else abs(o)/(abs(o)+100) if o else 0
def cast_odds(v):
    if v in (None,"",0): return 0
    if isinstance(v,(int,float)): return int(v)
    m=re.search(r"[-+]?\d+",str(v)); return int(m.group()) if m else 0

# ─────────────────  MAPS  ────────────────────────
futures_table_map = {
    ("Championship","NBA Championship"): "NBAChampionship",
    ("Conference Winner","Eastern Conference"): "NBAEasternConference",
    ("Conference Winner","Western Conference"): "NBAWesternConference",
    ("Defensive Player of Year Award","Award"): "NBADefensivePotY",
    ("Division Winner","Atlantic Division"): "NBAAtlantic",
    ("Division Winner","Central Division"):  "NBACentral",
    ("Division Winner","Northwest Division"):"NBANorthwest",
    ("Division Winner","Pacific Division"):  "NBAPacific",
    ("Division Winner","Southeast Division"): "NBASoutheast",
    ("Division Winner","Southwest Division"): "NBASouthwest",
    ("Most Improved Player Award","Award"):  "NBAMIP",
    ("Most Valuable Player Award","Award"):  "NBAMVP",
    ("Rookie of Year Award","Award"):        "NBARotY",
    ("Sixth Man of Year Award","Award"):     "NBASixthMotY",
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

sportsbook_cols = ["BetMGM","DraftKings","Caesars","ESPNBet","FanDuel","BallyBet","RiversCasino","Bet365"]

# ─────────────────  CORE UTILS  ────────────────
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
            (alias, cutoff_dt))
        row=cur.fetchone()
    if not row: return 1.0,0.0
    nums=[cast_odds(row[c]) for c in sportsbook_cols if cast_odds(row[c])]
    if not nums: return 1.0,0.0
    best=max(nums)
    return american_odds_to_decimal(best), american_odds_to_prob(best)

# ─────────────────  EV TABLE PAGE  ──────────────
def ev_table_page():
    st.header("EV Table")

    bet_conn, fut_conn = new_betting_conn(), new_futures_conn()
    now = datetime.utcnow()

    # ----- Active wagers -----
    with with_cursor(bet_conn) as cur:
        cur.execute("""
            SELECT b.WagerID,b.PotentialPayout,b.DollarsAtStake,
                   l.EventType,l.EventLabel,l.ParticipantName
              FROM bets b JOIN legs l ON b.WagerID=l.WagerID
             WHERE b.WhichBankroll='GreenAleph' AND b.WLCA='Active' AND l.LeagueName='NBA'""")
        rows=cur.fetchall()

    active=defaultdict(lambda:{"pot":0,"stake":0,"legs":[]})
    for r in rows:
        a=active[r["WagerID"]]
        a["pot"]=a["pot"] or float(r["PotentialPayout"] or 0)
        a["stake"]=a["stake"] or float(r["DollarsAtStake"] or 0)
        a["legs"].append((r["EventType"],r["EventLabel"],r["ParticipantName"]))

    active_stake,active_exp=defaultdict(float),defaultdict(float)
    for bet in active.values():
        pot,stake,legs=bet["pot"],bet["stake"],bet["legs"]
        decs=[]; prob=1.0
        for et,el,pn in legs:
            dec,p=best_odds_decimal_prob(et,el,pn,now,fut_conn)
            if p==0: prob=0; break
            decs.append((dec,et,el)); prob*=p
        if prob==0: continue
        expected=pot*prob
        sum_exc=sum(d-1 for d,_,_ in decs)
        if sum_exc<=0: continue
        for d,et,el in decs:
            w=(d-1)/sum_exc
            active_stake[(et,el)]+=w*stake
            active_exp  [(et,el)]+=w*expected

    # ----- Realised net profit -----
    with with_cursor(bet_conn) as cur:
        cur.execute("""
            SELECT b.WagerID,b.NetProfit,l.EventType,l.EventLabel,l.ParticipantName
              FROM bets b JOIN legs l ON b.WagerID=l.WagerID
             WHERE b.WhichBankroll='GreenAleph'
               AND b.WLCA IN ('Win','Loss','Cashout') AND l.LeagueName='NBA'""")
        rows=cur.fetchall()

    wager_net=defaultdict(float); wager_legs=defaultdict(list)
    for r in rows:
        wager_net[r["WagerID"]]=float(r["NetProfit"] or 0)
        wager_legs[r["WagerID"]].append((r["EventType"],r["EventLabel"],r["ParticipantName"]))

    realized=defaultdict(float)
    for wid,legs in wager_legs.items():
        net=wager_net[wid]
        decs=[(best_odds_decimal_prob(et,el,pn,now,fut_conn)[0],et,el) for et,el,pn in legs]
        sum_exc=sum(d-1 for d,_,_ in decs)
        if sum_exc<=0: continue
        for d,et,el in decs:
            realized[(et,el)]+=net*((d-1)/sum_exc)

    bet_conn.close(); fut_conn.close()

    # ----- table -----
    keys=set(active_stake)|set(active_exp)|set(realized)
    rows=[]
    for et,el in sorted(keys):
        stake=active_stake.get((et,el),0); exp=active_exp.get((et,el),0); net=realized.get((et,el),0)
        rows.append(dict(EventType=et,EventLabel=el,
                         ActiveDollarsAtStake=round(stake,2),
                         ActiveExpectedPayout=round(exp,2),
                         RealizedNetProfit=round(net,2),
                         ExpectedValue=round(exp-stake+net,2)))
    st.dataframe(pd.DataFrame(rows).sort_values(["EventType","EventLabel"]),
                 use_container_width=True)

# ─────────────────  % RETURN PLOT PAGE  ──────────
def return_plot_page_fast():
    st.header("% Return Plot")

    fut_conn, bet_conn = new_futures_conn(), new_betting_conn()

    ev_types=sorted({t for (t,_) in futures_table_map})
    sel_type=st.selectbox("Event Type",ev_types)
    labels=sorted({lbl for (t,lbl) in futures_table_map if t==sel_type})
    sel_lbl=st.selectbox("Event Label",labels)

    c1,c2=st.columns(2)
    start_date=c1.date_input("Start",datetime.utcnow().date()-timedelta(days=60))
    end_date  =c2.date_input("End"  ,datetime.utcnow().date())
    if start_date>end_date:
        st.error("Start date must precede end date"); return
    if not st.button("Generate Plot"): 
        st.info("Choose filters & press **Generate Plot**"); return

    # 1) active wagers
    with with_cursor(bet_conn) as cur:
        cur.execute("""
            SELECT b.WagerID,b.PotentialPayout,b.DollarsAtStake,b.DateTimePlaced,
                   l.EventType,l.EventLabel,l.ParticipantName
              FROM bets b JOIN legs l ON b.WagerID=l.WagerID
             WHERE b.WhichBankroll='GreenAleph' AND b.WLCA='Active' AND l.LeagueName='NBA'""")
        bet_rows=cur.fetchall()
    bet_df=pd.DataFrame(bet_rows)
    if bet_df.empty: st.warning("No active wagers"); return

    # weights for selected market
    sel_bets = bet_df.assign(IS_SEL=lambda d:(d["EventType"]==sel_type)&(d["EventLabel"]==sel_lbl))
    wgt = (sel_bets.groupby("WagerID")["IS_SEL"]
                  .apply(lambda s:1.0/len(s) if s.any() else 0)
                  .reset_index(name="Weight"))
    wgt = wgt[wgt["Weight"]>0]
    if wgt.empty: st.warning("No active legs match selection"); return

    # 2) odds snapshots (cached)
    participants=bet_df["ParticipantName"].map(lambda x:team_alias_map.get(x,x)).unique().tolist()
    tbl=futures_table_map[(sel_type,sel_lbl)]

    @st.cache_data(ttl=3600,show_spinner=False)
    def cached_odds(tbl,names,start,end):
        if not names: return pd.DataFrame()
        placeholders=",".join(["%s"]*len(names))
        with with_cursor(fut_conn) as cur:
            cur.execute(
                f"""SELECT team_name,date_created,{','.join(sportsbook_cols)}
                      FROM {tbl}
                     WHERE team_name IN ({placeholders})
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

    odds_df=cached_odds(tbl,participants,start_date,end_date)
    if odds_df.empty: st.warning("No odds data"); return

    # 3) assemble return
    bet_meta=bet_df[["WagerID","PotentialPayout","DollarsAtStake","DateTimePlaced"]].drop_duplicates()
    merged=(bet_df.merge(odds_df,how="left",left_on="ParticipantName",right_on="team_name")
                   .dropna(subset=["prob"])
                   .merge(wgt,on="WagerID",how="inner"))
    merged["date"]=pd.to_datetime(merged["date"])

    # ensure numeric BEFORE arithmetic
    for col in ["Weight","DollarsAtStake","PotentialPayout"]:
        merged[col]=pd.to_numeric(merged[col],errors="coerce").fillna(0.0)

    merged["stake_part"]=merged["DollarsAtStake"]*merged["Weight"]
    merged["exp_part"]=merged["PotentialPayout"]*merged["prob"]*merged["Weight"]
    merged["net_part"]=merged["exp_part"]-merged["stake_part"]

    series=(merged.groupby("date")
                  .agg(net=("net_part","sum"),stake=("stake_part","sum"))
                  .reset_index())
    series["pct"]=series["net"]/series["stake"]*100
    series=series[(series["date"].dt.date>=start_date)&(series["date"].dt.date<=end_date)]
    if series.empty.any(): st.warning("Insufficient data"); return

    fig,ax=plt.subplots(figsize=(10,5))
    ax.plot(series["date"],series["pct"],marker="o")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    ax.set_title(f"% Return — {sel_type}: {sel_lbl}")
    ax.set_xlabel("Date"); ax.set_ylabel("Return (%)")
    plt.xticks(rotation=45)
    st.pyplot(fig,use_container_width=True)

# ─────────────────  SIDEBAR NAV  ─────────────────
page=st.sidebar.radio("Choose Page",["EV Table","% Return Plot"])
if page=="EV Table":
    ev_table_page()
else:
    return_plot_page_fast()
