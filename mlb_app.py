import streamlit as st
import pymysql, pandas as pd, re
from collections import defaultdict
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# ───────────────────────── Page config ──────────────────────────
st.set_page_config(page_title="NBA Futures Dashboard", layout="wide")

# ─────────────────────── DB connection helpers ──────────────────
def get_betting_conn():
    return pymysql.connect(
        host=st.secrets["BETTING_DB"]["host"],
        user=st.secrets["BETTING_DB"]["user"],
        password=st.secrets["BETTING_DB"]["password"],
        database=st.secrets["BETTING_DB"]["database"],
        cursorclass=pymysql.cursors.DictCursor)

def get_futures_conn():
    return pymysql.connect(
        host=st.secrets["FUTURES_DB"]["host"],
        user=st.secrets["FUTURES_DB"]["user"],
        password=st.secrets["FUTURES_DB"]["password"],
        database=st.secrets["FUTURES_DB"]["database"],
        cursorclass=pymysql.cursors.DictCursor)

# ───────────────────────── Odds helpers ─────────────────────────
def american_odds_to_decimal(o):   return 1+o/100 if o>0 else 1+100/abs(o) if o else 1
def american_odds_to_probability(o): return 100/(o+100) if o>0 else abs(o)/(abs(o)+100) if o else 0
def safe_cast_odds(v):
    try:
        if isinstance(v,(int,float)): return int(v)
        if isinstance(v,str):
            m=re.search(r"[-+]?\d+",v);  return int(m.group()) if m else 0
    except: pass
    return 0

# ───────────────────────── static maps ──────────────────────────
futures_table_map={("Most Valuable Player Award","Award"):"NBAMVP",
("Championship","NBA Championship"):"NBAChampionship",
("Conference Winner","Eastern Conference"):"NBAEasternConference",
("Conference Winner","Western Conference"):"NBAWesternConference"}
team_alias_map={"Philadelphia 76ers":"76ers","Milwaukee Bucks":"Bucks","Boston Celtics":"Celtics",
"Denver Nuggets":"Nuggets"}           # cut for brevity, add others as needed
sports_cols=["BetMGM","DraftKings","Caesars","ESPNBet","FanDuel","BallyBet","RiversCasino","Bet365"]

# ───────────────────── Best odds (non‑zero) ─────────────────────
def get_best_decimal_and_prob(et,el,team,conn,date_cutoff=None):
    tbl=futures_table_map.get((et,el));  alias=team_alias_map.get(team,team)
    if not tbl: return 1.0,0.0
    with conn.cursor() as c:
        sql=f"SELECT {','.join(sports_cols)} FROM {tbl} WHERE team_name=%s"
        params=[alias]
        if date_cutoff:
            sql+=" AND date_created<=%s ORDER BY date_created DESC LIMIT 1"
            params.append(date_cutoff)
        else:
            sql+=" ORDER BY date_created DESC LIMIT 1"
        c.execute(sql,params); row=c.fetchone()
    if not row: return 1.0,0.0
    nums=[safe_cast_odds(row[col]) for col in sports_cols if safe_cast_odds(row[col])!=0]
    if not nums: return 1.0,0.0
    best=max(nums);  return american_odds_to_decimal(best),american_odds_to_probability(best)

# ───────────────────────── EV‑table page ────────────────────────
def ev_table_page():
    st.header("EV Table")
    bconn,fconn=get_betting_conn(),get_futures_conn()
    active,expected,realized=defaultdict(float),defaultdict(float),defaultdict(float)

    # ------- Active -------
    with bconn.cursor() as c:
        c.execute("""SELECT b.WagerID,b.PotentialPayout,b.DollarsAtStake,
                     l.EventType,l.EventLabel,l.ParticipantName
                     FROM bets b JOIN legs l USING(WagerID)
                     WHERE b.WhichBankroll='GreenAleph'
                     AND b.WLCA='Active' AND l.LeagueName='NBA'""")
        rows=c.fetchall()
    bets=defaultdict(lambda:{"pot":0,"stake":0,"legs":[]})
    for r in rows:
        bet=bets[r["WagerID"]]
        bet["pot"]=bet["pot"] or float(r["PotentialPayout"] or 0)
        bet["stake"]=bet["stake"] or float(r["DollarsAtStake"] or 0)
        bet["legs"].append((r["EventType"],r["EventLabel"],r["ParticipantName"]))
    for bet in bets.values():
        pot,stake,legs=bet["pot"],bet["stake"],bet["legs"]
        decs=[];   parlay_prob=1
        for et,el,pn in legs:
            d,p=get_best_decimal_and_prob(et,el,pn,fconn)
            decs.append((d,et,el)); parlay_prob*=p
        if parlay_prob==0: continue
        exp=pot*parlay_prob; s_ex=sum(d-1 for d,_,_ in decs);  # M_i -1 sum
        if s_ex<=0: continue
        for d,et,el in decs:
            f=(d-1)/s_ex
            active[(et,el)]+=f*stake
            expected[(et,el)]+=f*exp

    # ------- Realized (Win/Loss/Cashout) -------
    with bconn.cursor() as c:
        c.execute("""SELECT b.WagerID,b.NetProfit,l.EventType,l.EventLabel,l.ParticipantName
                     FROM bets b JOIN legs l USING(WagerID)
                     WHERE b.WhichBankroll='GreenAleph'
                     AND b.WLCA IN ('Win','Loss','Cashout') AND l.LeagueName='NBA'""")
        rows=c.fetchall()
    by_wager=defaultdict(list)
    for r in rows: by_wager[r["WagerID"]].append(r)
    for leglist in by_wager.values():
        net=leglist[0]["NetProfit"] or 0
        decs=[(get_best_decimal_and_prob(l["EventType"],l["EventLabel"],l["ParticipantName"],fconn)[0],
               l["EventType"],l["EventLabel"]) for l in leglist]
        s_ex=sum(d-1 for d,_,_ in decs);  # same allocation trick
        if s_ex<=0: continue
        for d,et,el in decs:
            realized[(et,el)]+=net*((d-1)/s_ex)

    # ------- assemble -------
    rec=[]
    for k in set(active)|set(expected)|set(realized):
        stake=active.get(k,0); exp=expected.get(k,0); net=realized.get(k,0)
        rec.append({"EventType":k[0],"EventLabel":k[1],
                    "ActiveDollarsAtStake":round(stake,2),
                    "ActiveExpectedPayout":round(exp,2),
                    "RealizedNetProfit":round(net,2),
                    "ExpectedValue":round(exp-stake+net,2)})
    df=pd.DataFrame(rec).sort_values(["EventType","EventLabel"])
    st.dataframe(df,use_container_width=True)

# ─────────────────────── Return‑plot page ───────────────────────
def return_plot_page():
    st.header("% Return Plot")
    event_types=sorted(set(k[0] for k in futures_table_map))
    etype=st.selectbox("Event Type",event_types,index=0)
    labels=[lbl for (et,lbl) in futures_table_map if et==etype]
    elabel=st.selectbox("Event Label",labels,index=0)

    col1,col2=st.columns(2)
    with col1:
        start=st.date_input("Start",datetime.today().date()-timedelta(days=30))
    with col2:
        end=st.date_input("End",datetime.today().date())
    if start>end:
        st.error("Start must be ≤ End"); return

    if st.button("Generate Plot"):
        bconn,fconn=get_betting_conn(),get_futures_conn()

        # pull ACTIVE bets once
        with bconn.cursor() as c:
            c.execute("""SELECT b.WagerID,b.PotentialPayout,b.DollarsAtStake,b.DateTimePlaced,
                         l.EventType,l.EventLabel,l.ParticipantName
                         FROM bets b JOIN legs l USING(WagerID)
                         WHERE b.WhichBankroll='GreenAleph'
                         AND b.WLCA='Active' AND l.LeagueName='NBA'""")
            rows=c.fetchall()

        # structure bets
        bets=defaultdict(lambda:{"pot":0,"stake":0,"placed":None,"legs":[]})
        for r in rows:
            w=bets[r["WagerID"]]
            w["pot"]=r["PotentialPayout"]; w["stake"]=r["DollarsAtStake"]
            placed=r["DateTimePlaced"]
            # ensure datetime obj
            if isinstance(placed,str): placed=pd.to_datetime(placed)
            w["placed"]=placed
            w["legs"].append((r["EventType"],r["EventLabel"],r["ParticipantName"]))

        series=[]
        for date in pd.date_range(start,end):
            snap=datetime.combine(date,datetime.max.time())
            tot_net=tot_stake=0.0
            for bet in bets.values():
                if bet["placed"] is None or bet["placed"]>snap: continue
                parlay_prob=1; decs=[]
                for et,el,pn in bet["legs"]:
                    dec,prob=get_best_decimal_and_prob(et,el,pn,fconn,snap)
                    if prob==0: parlay_prob=0; break
                    decs.append((dec,et,el)); parlay_prob*=prob
                if parlay_prob==0: continue
                net=(bet["pot"]*parlay_prob)-bet["stake"]
                s_ex=sum(d-1 for d,_,_ in decs);  # allocation base
                if s_ex<=0: continue
                for d,et,el in decs:
                    if et==etype and el==elabel:
                        frac=(d-1)/s_ex
                        tot_net+=frac*net; tot_stake+=frac*bet["stake"]
            ret= (tot_net/tot_stake*100) if tot_stake>0 else 0
            series.append((date,ret))

        if not series or all(v==0 for _,v in series):
            st.info("No data for selected filters."); return

        # plot
        dates,vals=zip(*series)
        fig,ax=plt.subplots(figsize=(9,5))
        ax.plot(dates,vals,'o-')
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
        ax.set_title(f"% Return – {etype} / {elabel}")
        ax.set_ylabel("Return (%)"); ax.set_xlabel("Date")
        plt.xticks(rotation=45); plt.tight_layout()
        st.pyplot(fig)

# ───────────────────────── Routing ──────────────────────────────
page=st.sidebar.radio("Page",("EV Table","% Return Plot"))
if page=="EV Table":  ev_table_page()
else:                 return_plot_page()
