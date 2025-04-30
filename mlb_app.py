import streamlit as st
import pymysql, re
from collections import defaultdict
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

st.set_page_config(page_title="NBA Futures Dashboard", layout="wide")

# ────────── DB HELPERS ──────────

def new_betting_conn():
    return pymysql.connect(**st.secrets["BETTING_DB"], cursorclass=pymysql.cursors.DictCursor, autocommit=True)

def new_futures_conn():
    return pymysql.connect(**st.secrets["FUTURES_DB"], cursorclass=pymysql.cursors.DictCursor, autocommit=True)

def with_cursor(conn):
    conn.ping(reconnect=True)
    return conn.cursor()

# ────────── ODDS & UTIL ──────────

def american_odds_to_decimal(o:int)->float:
    return 1 + (o/100) if o>0 else 1 + 100/abs(o) if o else 1

def american_odds_to_prob(o:int)->float:
    return 100/(o+100) if o>0 else abs(o)/(abs(o)+100) if o else 0

def cast_odds(v):
    if v in (None, "", 0):
        return 0
    if isinstance(v, (int, float)):
        return int(v)
    m = re.search(r"[-+]?\d+", str(v))
    return int(m.group()) if m else 0

# ────────── MAPS ──────────

futures_table_map = {
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

team_alias_map = {n:n.split()[-1] if n.startswith("Los") else n.split(maxsplit=1)[-1] for n in [
    "Atlanta Hawks","Boston Celtics","Brooklyn Nets","Charlotte Hornets","Chicago Bulls","Cleveland Cavaliers","Dallas Mavericks","Denver Nuggets","Detroit Pistons","Golden State Warriors","Houston Rockets","Indiana Pacers","LA Clippers","Los Angeles Clippers","Los Angeles Lakers","Memphis Grizzlies","Miami Heat","Milwaukee Bucks","Minnesota Timberwolves","New Orleans Pelicans","New York Knicks","Oklahoma City Thunder","Orlando Magic","Philadelphia 76ers","Phoenix Suns","Portland Trail Blazers","Sacramento Kings","San Antonio Spurs","Toronto Raptors","Utah Jazz","Washington Wizards"]}

sportsbook_cols = ["BetMGM","DraftKings","Caesars","ESPNBet","FanDuel","BallyBet","RiversCasino","Bet365"]

# ────────── CORE HELPER ──────────

def best_odds_decimal_prob(event_type,event_label,participant,cutoff_dt,fut_conn):
    tbl=futures_table_map.get((event_type,event_label))
    if not tbl:
        return 1.0,0.0
    alias=team_alias_map.get(participant,participant)
    with with_cursor(fut_conn) as cur:
        cur.execute(
            f"SELECT {','.join(sportsbook_cols)} FROM {tbl} WHERE team_name=%s AND date_created<=%s ORDER BY date_created DESC LIMIT 1",
            (alias,cutoff_dt))
        row=cur.fetchone()
    if not row:
        return 1.0,0.0
    nums=[cast_odds(row[c]) for c in sportsbook_cols if cast_odds(row[c])]
    if not nums:
        return 1.0,0.0
    best=max(nums)
    return american_odds_to_decimal(best),american_odds_to_prob(best)

# ────────── EV TABLE (same as before, omitted for brevity) ──────────

def ev_table_page():
    st.write("EV Table placeholder – loads fine in other version")

# ────────── ODDS MOVEMENT PAGE ──────────

def odds_movement_page():
    st.subheader("Weekly Odds Movement (Top‑5)")
    fut=new_futures_conn()

    etype=st.selectbox("Event Type",sorted({t for t,_ in futures_table_map}),key='om1')
    elabel=st.selectbox("Event Label",sorted({l for t,l in futures_table_map if t==etype}),key='om2')

    col1,col2=st.columns(2)
    sd=col1.date_input("Start",datetime.utcnow().date()-timedelta(120))
    ed=col2.date_input("End",datetime.utcnow().date())
    if sd>ed:
        st.error("Start after End"); return

    if not st.button("Plot",key='plot'): return

    tbl=futures_table_map[(etype,elabel)]
    with with_cursor(fut) as cur:
        cur.execute(
            f"SELECT team_name,date_created,{','.join(sportsbook_cols)} FROM {tbl} WHERE date_created BETWEEN %s AND %s",
            (f"{sd} 00:00:00",f"{ed} 23:59:59"))
        raw=pd.DataFrame(cur.fetchall())
    if raw.empty:
        st.warning("No odds data"); return

    raw[sportsbook_cols]=raw[sportsbook_cols].apply(pd.to_numeric,errors='coerce').fillna(0)
    raw['best']=raw[sportsbook_cols].replace(0,pd.NA).max(axis=1).fillna(0).astype(int)
    raw['prob']=raw['best'].apply(american_odds_to_prob)
    raw['date']=pd.to_datetime(raw['date_created']).dt.date

    # de‑duplicate snapshots per day
    raw=raw.sort_values(['team_name','date']).drop_duplicates(['team_name','date'])
    raw['week']=pd.to_datetime(raw['date']).dt.to_period('W').dt.start_time

    weekly=(raw.groupby('team_name',group_keys=False)
              .apply(lambda g: g.set_index('week').asfreq('W-MON').ffill())
              .reset_index())

    if weekly.empty:
        st.warning("No weekly data"); return

    last_week=weekly['week'].max()
    top5_names=(weekly[weekly['week']==last_week]
                 .nlargest(5,'prob')['team_name']
                 .tolist())
    top5=weekly[weekly['team_name'].isin(top5_names)]

    fig,ax=plt.subplots(figsize=(11,6))
    for name,grp in top5.groupby('team_name'):
        ax.plot(grp['week'],grp['prob']*100,marker='o',linewidth=2,label=name)

    ax.set_title(f"{elabel} – Weekly Implied Probability (Top‑5)")
    ax.set_xlabel("Week"); ax.set_ylabel("Probability (%)")
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
    plt.xticks(rotation=45); ax.legend(title="Name",bbox_to_anchor=(1.02,1),loc='upper left')
    ax.grid(True,alpha=0.3)
    st.pyplot(fig,use_container_width=True)

# ────────── SIDEBAR NAV ──────────

page=st.sidebar.radio("Choose Page",["EV Table","Odds Movement Plot"])
if page=="EV Table":
    ev_table_page()
else:
    odds_movement_page()
