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
        host=st.secrets["BETTING_DB"]["host"],
        user=st.secrets["BETTING_DB"]["user"],
        password=st.secrets["BETTING_DB"]["password"],
        database=st.secrets["BETTING_DB"]["database"],
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )

def new_futures_conn():
    return pymysql.connect(
        host=st.secrets["FUTURES_DB"]["host"],
        user=st.secrets["FUTURES_DB"]["user"],
        password=st.secrets["FUTURES_DB"]["password"],
        database=st.secrets["FUTURES_DB"]["database"],
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )

def with_cursor(conn):
    """Ping first, always close cursor."""
    conn.ping(reconnect=True)
    return conn.cursor()

# ─────────────────  ODDS UTILS  ──────────────────
def american_odds_to_decimal(o:int)->float:
    return 1.0 + (o/100) if o>0 else 1.0 + 100/abs(o) if o else 1.0

def american_odds_to_prob(o:int)->float:
    return 100/(o+100) if o>0 else abs(o)/(abs(o)+100) if o else 0.0

def cast_odds(v):
    if v in (None,"",0): return 0
    if isinstance(v,(int,float)): return int(v)
    m=re.search(r"[-+]?\d+",str(v))
    return int(m.group()) if m else 0

# ─────────────────  MAPPINGS  ────────────────────
futures_table_map = {
    ("Championship","NBA Championship"):            "NBAChampionship",
    ("Conference Winner","Eastern Conference"):     "NBAEasternConference",
    ("Conference Winner","Western Conference"):     "NBAWesternConference",
    ("Defensive Player of Year Award","Award"):     "NBADefensivePotY",
    ("Division Winner","Atlantic Division"):        "NBAAtlantic",
    ("Division Winner","Central Division"):         "NBACentral",
    ("Division Winner","Northwest Division"):       "NBANorthwest",
    ("Division Winner","Pacific Division"):         "NBAPacific",
    ("Division Winner","Southeast Division"):       "NBASoutheast",
    ("Division Winner","Southwest Division"):       "NBASouthwest",
    ("Most Improved Player Award","Award"):         "NBAMIP",
    ("Most Valuable Player Award","Award"):         "NBAMVP",
    ("Rookie of Year Award","Award"):               "NBARotY",
    ("Sixth Man of Year Award","Award"):            "NBASixthMotY",
}

# City-prefixed name  → short name exactly as stored in futures_db
team_alias_map = {
    "Philadelphia 76ers":"76ers","Milwaukee Bucks":"Bucks","Chicago Bulls":"Bulls",
    "Cleveland Cavaliers":"Cavaliers","Boston Celtics":"Celtics",
    "Los Angeles Clippers":"Clippers","Memphis Grizzlies":"Grizzlies",
    "Atlanta Hawks":"Hawks","Miami Heat":"Heat","Charlotte Hornets":"Hornets",
    "Utah Jazz":"Jazz","Sacramento Kings":"Kings","New York Knicks":"Knicks",
    "Los Angeles Lakers":"Lakers","Orlando Magic":"Magic","Dallas Mavericks":"Mavericks",
    "Brooklyn Nets":"Nets","Denver Nuggets":"Nuggets","Indiana Pacers":"Pacers",
    "New Orleans Pelicans":"Pelicans","Detroit Pistons":"Pistons","Toronto Raptors":"Raptors",
    "Houston Rockets":"Rockets","San Antonio Spurs":"Spurs","Phoenix Suns":"Suns",
    "Oklahoma City Thunder":"Thunder","Minnesota Timberwolves":"Timberwolves",
    "Portland Trail Blazers":"Trail Blazers","Golden State Warriors":"Warriors",
    "Washington Wizards":"Wizards",
}

sportsbook_cols = ["BetMGM","DraftKings","Caesars","ESPNBet","FanDuel",
                   "BallyBet","RiversCasino","Bet365"]

# ─────────────────  BEST-ODDS LOOKUP  ────────────
def best_odds_decimal_prob(event_type,event_label,participant,cutoff_dt,fut_conn):
    table = futures_table_map.get((event_type,event_label))
    if not table: return 1.0,0.0
    alias = team_alias_map.get(participant, participant)   # players unchanged
    with with_cursor(fut_conn) as cur:
        cur.execute(
            f"""SELECT {','.join(sportsbook_cols)}
                  FROM {table}
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

# ─────────────────  EV TABLE PAGE  ───────────────
def ev_table_page():
    st.header("EV Table")
    bet_conn, fut_conn = new_betting_conn(), new_futures_conn()
    now = datetime.utcnow()

    # Active wagers
    with with_cursor(bet_conn) as c:
        c.execute("""
            SELECT b.WagerID,b.PotentialPayout,b.DollarsAtStake,
                   l.EventType,l.EventLabel,l.ParticipantName
              FROM bets b JOIN legs l ON b.WagerID=l.WagerID
             WHERE b.WhichBankroll='GreenAleph' AND b.WLCA='Active' AND l.LeagueName='NBA'
        """)
        rows=c.fetchall()
    act=defaultdict(lambda:{"pot":0,"stake":0,"legs":[]})
    for r in rows:
        d=act[r["WagerID"]]
        d["pot"]=d["pot"] or float(r["PotentialPayout"] or 0)
        d["stake"]=d["stake"] or float(r["DollarsAtStake"] or 0)
        d["legs"].append((r["EventType"],r["EventLabel"],r["ParticipantName"]))

    a_stake,a_exp=defaultdict(float),defaultdict(float)
    for d in act.values():
        pot,stake,legs=d["pot"],d["stake"],d["legs"]
        decs=[]; prob=1.0
        for et,el,pn in legs:
            dec,p=best_odds_decimal_prob(et,el,pn,now,fut_conn)
            if p==0: prob=0; break
            decs.append((dec,et,el)); prob*=p
        if prob==0: continue
        exp=pot*prob
        sum_exc=sum(dec-1 for dec,_,_ in decs)
        if sum_exc<=0: continue
        for dec,et,el in decs:
            w=(dec-1)/sum_exc
            a_stake[(et,el)]+=w*stake
            a_exp  [(et,el)]+=w*exp

    # Realised stakes
    with with_cursor(bet_conn) as c:
        c.execute("""
            SELECT b.WagerID,b.NetProfit,
                   l.EventType,l.EventLabel,l.ParticipantName
              FROM bets b JOIN legs l ON b.WagerID=l.WagerID
             WHERE b.WhichBankroll='GreenAleph' AND b.WLCA IN ('Win','Loss','Cashout')
               AND l.LeagueName='NBA'
        """)
        rows=c.fetchall()
    w_net=defaultdict(float); w_legs=defaultdict(list)
    for r in rows:
        w_net[r["WagerID"]]=float(r["NetProfit"] or 0)
        w_legs[r["WagerID"]].append((r["EventType"],r["EventLabel"],r["ParticipantName"]))
    r_net=defaultdict(float)
    for wid,legs in w_legs.items():
        net=w_net[wid]
        decs=[(best_odds_decimal_prob(et,el,pn,now,fut_conn)[0],et,el) for et,el,pn in legs]
        sum_exc=sum(d-1 for d,_,_ in decs)
        if sum_exc<=0: continue
        for dec,et,el in decs:
            r_net[(et,el)]+=net*((dec-1)/sum_exc)

    bet_conn.close(); fut_conn.close()

    data=[]
    for key in sorted(set(a_stake)|set(a_exp)|set(r_net)):
        et,el=key
        stake=a_stake.get(key,0); exp=a_exp.get(key,0); net=r_net.get(key,0)
        data.append(dict(EventType=et,EventLabel=el,
                         ActiveDollarsAtStake=round(stake,2),
                         ActiveExpectedPayout=round(exp,2),
                         RealizedNetProfit=round(net,2),
                         ExpectedValue=round(exp-stake+net,2)))
    st.dataframe(pd.DataFrame(data).sort_values(["EventType","EventLabel"]),
                 use_container_width=True)

# ─────────────────  % RETURN PLOT PAGE  ──────────
def return_plot_page():
    st.header("% Return Plot")
    fut_conn, bet_conn = new_futures_conn(), new_betting_conn()

    sel_type = st.selectbox("Event Type", sorted({t for (t, _) in futures_table_map}))
    sel_lbl  = st.selectbox("Event Label",
                            sorted(lbl for (t,lbl) in futures_table_map if t==sel_type))
    c1,c2=st.columns(2)
    start=c1.date_input("Start", datetime.utcnow().date()-timedelta(days=60))
    end  =c2.date_input("End",   datetime.utcnow().date())
    if start>end: st.error("Start must be ≤ End"); return
    if not st.button("Generate Plot"): return

    # ----- betting data -----
    with with_cursor(bet_conn) as c:
        c.execute("""
            SELECT b.WagerID,b.PotentialPayout,b.DollarsAtStake,b.DateTimePlaced,
                   l.EventType,l.EventLabel,l.ParticipantName
              FROM bets b JOIN legs l ON b.WagerID=l.WagerID
             WHERE b.WhichBankroll='GreenAleph' AND b.WLCA='Active' AND l.LeagueName='NBA'
        """)
        bets=pd.DataFrame(c.fetchall())
    if bets.empty: st.warning("No active wagers"); return

    bets["Alias"]=bets["ParticipantName"].apply(lambda x: team_alias_map.get(x,x))
    bets["is_sel"]=(bets["EventType"]==sel_type)&(bets["EventLabel"]==sel_lbl)

    # weight = 1/legs if wager contains selected market else 0
    weights=(bets.groupby("WagerID")
                 .apply(lambda g: 1/len(g) if g["is_sel"].any() else 0)
                 .rename("Weight")
                 .reset_index())
    weights=weights[weights["Weight"]>0]
    if weights.empty: st.warning("No wager contains selected market"); return
    st.caption(f"{len(weights)} active wagers include that market.")

    # ----- odds snapshots -----
    aliases_needed=bets.loc[bets["WagerID"].isin(weights["WagerID"]),"Alias"].unique().tolist()
    tbl=futures_table_map[(sel_type,sel_lbl)]

    @st.cache_data(ttl=3600, show_spinner=False)
    def load_odds(table,names,start_dt,end_dt):
        if not names: return pd.DataFrame()
        ph=", ".join(["%s"]*len(names))
        with with_cursor(fut_conn) as c:
            c.execute(
                f"""SELECT team_name,date_created,{','.join(sportsbook_cols)}
                      FROM {table}
                     WHERE team_name IN ({ph})
                       AND date_created BETWEEN %s AND %s
                     ORDER BY team_name,date_created""",
                (*names, f"{start_dt} 00:00:00", f"{end_dt} 23:59:59"))
            raw=pd.DataFrame(c.fetchall())
        if raw.empty: return raw
        raw[sportsbook_cols]=raw[sportsbook_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
        raw["best"]=(raw[sportsbook_cols].replace(0,pd.NA).max(axis=1).fillna(0).astype(int))
        raw["prob"]=raw["best"].apply(american_odds_to_prob)
        raw["date"]=pd.to_datetime(raw["date_created"]).dt.date
        return (raw.sort_values(["team_name","date"])
                    .groupby(["team_name","date"])
                    .tail(1))[["team_name","date","prob"]]

    odds=load_odds(tbl, aliases_needed, start, end)
    if odds.empty: st.warning("No odds data for that period"); return

    merged=(bets.merge(odds,how="left", left_on="Alias", right_on="team_name")
                .merge(weights,on="WagerID",how="inner"))
    merged.dropna(subset=["prob"], inplace=True)
    if merged.empty: st.warning("No odds overlap with wagers"); return
    merged["date"]=pd.to_datetime(merged["date"])

    daily=(merged.groupby(["date","WagerID"])
                 .agg(prob=("prob","prod"), weight=("Weight","first"))
                 .reset_index())
    meta=bets[["WagerID","PotentialPayout","DollarsAtStake"]].drop_duplicates()
    daily=daily.merge(meta,on="WagerID")
    daily[["weight","DollarsAtStake","PotentialPayout"]]=daily[["weight","DollarsAtStake","PotentialPayout"]].apply(pd.to_numeric, errors="coerce").fillna(0)

    daily["stake_part"]=daily["DollarsAtStake"]*daily["weight"]
    daily["exp_part"]  =daily["PotentialPayout"]*daily["prob"]*daily["weight"]
    daily["net_part"]  =daily["exp_part"]-daily["stake_part"]

    series=(daily.groupby("date")
                 .agg(net=("net_part","sum"), stake=("stake_part","sum"))
                 .reset_index())
    series["pct"]=series.apply(lambda r:(r["net"]/r["stake"]*100) if r["stake"] else None, axis=1)
    series=series.dropna(subset=["pct"])
    series=series[(series["date"].dt.date>=start)&(series["date"].dt.date<=end)]
    if series.empty: st.warning("Insufficient data"); return

    fig,ax=plt.subplots(figsize=(10,5))
    ax.plot(series["date"],series["pct"],marker="o")
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
    ax.set_title(f"% Return — {sel_type}: {sel_lbl}")
    ax.set_xlabel("Date"); ax.set_ylabel("Return (%)")
    plt.xticks(rotation=45)
    st.pyplot(fig, use_container_width=True)

# ─────────────────  NAVIGATION  ──────────────────
page = st.sidebar.radio("Choose Page", ["EV Table", "% Return Plot"])
(ev_table_page if page=="EV Table" else return_plot_page)()
