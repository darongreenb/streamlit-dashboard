import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from datetime import datetime, timedelta
import pymysql
import re
from collections import defaultdict
import traceback

# ────────────────────────────────────────────────────────────────────
# PAGE CONFIG
st.set_page_config(page_title="NBA Futures Historical EV", layout="wide")
st.markdown("<h1 style='text-align:center'>NBA Futures Historical EV</h1>", unsafe_allow_html=True)
st.markdown("<h3 style='text-align:center;color:gray'>Weekly EV Tracking</h3>", unsafe_allow_html=True)

# ────────────────────────────────────────────────────────────────────
# DB HELPERS (same as EV Table page)
def new_betting_conn():
    try:
        return pymysql.connect(
            host=st.secrets['BETTING_DB']['host'],
            user=st.secrets['BETTING_DB']['user'],
            password=st.secrets['BETTING_DB']['password'],
            database=st.secrets['BETTING_DB']['database'],
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True,
            connect_timeout=10
        )
    except pymysql.Error as e:
        st.error(f"Betting DB conn error {e.args[0]}")
        return None

def new_futures_conn():
    try:
        return pymysql.connect(
            host=st.secrets['FUTURES_DB']['host'],
            user=st.secrets['FUTURES_DB']['user'],
            password=st.secrets['FUTURES_DB']['password'],
            database=st.secrets['FUTURES_DB']['database'],
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True,
            connect_timeout=10
        )
    except pymysql.Error as e:
        st.error(f"Futures DB conn error {e.args[0]}")
        return None

def with_cursor(conn):
    if conn is None: return None
    try:
        conn.ping(reconnect=True)
        return conn.cursor()
    except Exception:
        return None

# ────────────────────────────────────────────────────────────────────
# ODDS HELPERS & MAPS (same as EV Table page)
def american_odds_to_decimal(o): return 1.0 + (o/100) if o>0 else 1.0 + 100/abs(o) if o else 1.0
 def american_odds_to_prob(o): return 100/(o+100) if o>0 else abs(o)/(abs(o)+100) if o else 0.0
def cast_odds(v):
    if v in (None, '', 0): return 0
    if isinstance(v,(int,float)): return int(v)
    m=re.search(r"[-+]?\d+",str(v))
    return int(m.group()) if m else 0

futures_table_map={
    ('Championship','NBA Championship'):'NBAChampionship',
    ('Conference Winner','Eastern Conference'):'NBAEasternConference',
    ('Conference Winner','Western Conference'):'NBAWesternConference',
    ('Defensive Player of Year Award','Award'):'NBADefensivePotY',
    ('Division Winner','Atlantic Division'):'NBAAtlantic',
    ('Division Winner','Central Division'):'NBACentral',
    ('Division Winner','Northwest Division'):'NBANorthwest',
    ('Division Winner','Pacific Division'):'NBAPacific',
    ('Division Winner','Southeast Division'):'NBASoutheast',
    ('Division Winner','Southwest Division'):'NBASouthwest',
    ('Most Improved Player Award','Award'):'NBAMIP',
    ('Most Valuable Player Award','Award'):'NBAMVP',
    ('Rookie of Year Award','Award'):'NBARotY',
    ('Sixth Man of Year Award','Award'):'NBASixthMotY',
}
team_alias_map={
    'Philadelphia 76ers':'76ers','Milwaukee Bucks':'Bucks','Chicago Bulls':'Bulls',
    # ... include all mappings as in EV table
}
sportsbook_cols=["BetMGM","DraftKings","Caesars","ESPNBet","FanDuel","BallyBet","RiversCasino","Bet365"]

def best_odds_decimal_prob(event_type,event_label,participant,cutoff_dt,fut_conn,vig_map):
    tbl=futures_table_map.get((event_type,event_label))
    if not tbl or fut_conn is None: return 1.0,0.0
    alias=team_alias_map.get(participant,participant)
    cur=with_cursor(fut_conn)
    if cur is None: return 1.0,0.0
    cur.execute(f"SELECT {','.join(sportsbook_cols)} FROM {tbl} WHERE team_name=%s AND date_created<=%s ORDER BY date_created DESC LIMIT 1",(alias,cutoff_dt))
    row=cur.fetchone()
    if not row: return 1.0,0.0
    nums=[cast_odds(row[c]) for c in sportsbook_cols]; nums=[n for n in nums if n]
    if not nums: return 1.0,0.0
    best=max(nums)
    dec=american_odds_to_decimal(best)
    prob=american_odds_to_prob(best)
    vig=vig_map.get((event_type,event_label),0.05)
    return dec,prob*(1-vig)

# ────────────────────────────────────────────────────────────────────
# SQL QUERIES (identical to EV Table)
sql_active = """
SELECT b.WagerID,b.PotentialPayout,b.DollarsAtStake,l.EventType,l.EventLabel,l.ParticipantName
FROM bets b JOIN legs l ON b.WagerID=l.WagerID
WHERE b.WhichBankroll='GreenAleph' AND b.WLCA='Active' AND l.LeagueName='NBA'
"""

sql_real = """
SELECT b.WagerID,b.NetProfit,l.EventType,l.EventLabel,l.ParticipantName
FROM bets b JOIN legs l ON b.WagerID=l.WagerID
WHERE b.WhichBankroll='GreenAleph' AND b.WLCA IN ('Win','Loss','Cashout') AND l.LeagueName='NBA'
"""

# ────────────────────────────────────────────────────────────────────
# HISTORICAL EV PAGE

def historical_ev_page():
    bet_conn=new_betting_conn(); fut_conn=new_futures_conn()
    if not bet_conn or not fut_conn:
        st.warning("Database connection failed. Demo data."); return
    # date pickers...
    start=st.sidebar.date_input("Start", datetime.utcnow().date()-timedelta(days=180))
    end=st.sidebar.date_input("End", datetime.utcnow().date())
    if start>end: st.error("Start after End"); return
    # vig inputs...
    default_vig=st.sidebar.slider("Default Vig %",0,20,5)
    vig_map={k:st.sidebar.slider(f"{et}–{el}",0,20,default_vig)/100 for et,el in futures_table_map}

    dates=[start+timedelta(days=7*i) for i in range(((end-start).days//7)+1)]
    ev_records=[]
    for dt in dates:
        now_dt=datetime.combine(dt,datetime.min.time())
        # fetch legs active and realized using sql_active/sql_real and with_cursor
        # replicate EV table logic: build active_bets dict, compute active_exp and realized_np per market
        # then for portfolio: EV = sum(active_exp)-sum(active_stake)+sum(realized_np)
        active_bets=defaultdict(lambda:{'pot':0,'stake':0,'legs':[]})
        cur=with_cursor(bet_conn); cur.execute(sql_active)
        for r in cur.fetchall():
            # include only those placed <= now_dt
            if r['WagerID']:
                active_bets[r['WagerID']]['pot'] = r['PotentialPayout']
                active_bets[r['WagerID']]['stake'] = r['DollarsAtStake']
                active_bets[r['WagerID']]['legs'].append((r['EventType'],r['EventLabel'],r['ParticipantName']))
        active_stake=defaultdict(float); active_exp=defaultdict(float)
        for pot_data in active_bets.values():
            pot,stake,legs=pot_data['pot'],pot_data['stake'],pot_data['legs']
            decs=[];prob=1.0
            for et,el,pn in legs:
                dec,p=best_odds_decimal_prob(et,el,pn,now_dt,fut_conn,vig_map)
                decs.append(dec); prob*=p
            if prob>0:
                expected=pot*prob; tot_exc=sum(d-1 for d in decs)
                for d in decs:
                    w=(d-1)/tot_exc; active_stake['port']+=w*stake; active_exp['port']+=w*expected
        # realized
        realized_np=defaultdict(float)
        cur.execute(sql_real)
        for r in cur.fetchall():
            net=float(r['NetProfit']); decs=[]
            decs=[best_odds_decimal_prob(r['EventType'],r['EventLabel'],r['ParticipantName'],now_dt,fut_conn,vig_map)[0]]
            tot_exc=sum(d-1 for d in decs)
            for d in decs: realized_np['port']+=net*((d-1)/tot_exc)
        total_ev=sum(active_exp.values())-sum(active_stake.values())+sum(realized_np.values())
        ev_records.append({'Date':now_dt,'Total EV':total_ev})
    df=pd.DataFrame(ev_records)
    # plot with Plotly same as before
    fig=go.Figure(go.Scatter(x=df['Date'],y=df['Total EV'],mode='lines+markers',name='Total EV',line=dict(color='green')))
    fig.update_layout(title='Weekly NBA Futures EV',template='plotly_white')
    st.plotly_chart(fig,use_container_width=True)

if __name__=='__main__':
    historical_ev_page()
