import streamlit as st
import pymysql
import pandas as pd
import numpy as np
import re
from datetime import datetime, timedelta
from collections import defaultdict
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# ──────────────────────────────────────────────
# PAGE CONFIG
# ──────────────────────────────────────────────
st.set_page_config(page_title="NBA Futures – Portfolio EV Over Time", layout="wide")
st.markdown("""
<h1 style='text-align:center'>NBA Futures — Portfolio Expected Value Trend</h1>
<p style='text-align:center;color:gray'>Rolling 7‑day snapshots, using the same EV math as the EV table.</p>
""", unsafe_allow_html=True)

# ──────────────────────────────────────────────
# DB CONFIG (directly embedded instead of using Streamlit secrets)
# ──────────────────────────────────────────────
BETTING_CFG = {
    "host": "betting-db.cp86ssaw6cm7.us-east-1.rds.amazonaws.com",
    "user": "admin",
    "password": "7nRB1i2&A-K>",
    "database": "betting_db",
    "cursorclass": pymysql.cursors.DictCursor,
    "autocommit": True
}

FUTURES_CFG = {
    "host": "greenalephfutures.cnwukek8ge3b.us-east-2.rds.amazonaws.com",
    "user": "admin",
    "password": "greenalephadmin",
    "database": "futuresdata",
    "cursorclass": pymysql.cursors.DictCursor,
    "autocommit": True
}

def new_conn(cfg):
    return pymysql.connect(**cfg)

# ──────────────────────────────────────────────
# FUTURES MARKETS & TEAM ALIASES
# ──────────────────────────────────────────────
sportsbooks = ["BetMGM","DraftKings","Caesars","ESPNBet","FanDuel","BallyBet","RiversCasino","Bet365"]

FUTURES_TABLE = {
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

TEAM_ALIAS = {n: n.split()[-1] if n.startswith("Los") else n.split(maxsplit=1)[-1] for n in [
    "Atlanta Hawks","Boston Celtics","Brooklyn Nets","Charlotte Hornets","Chicago Bulls","Cleveland Cavaliers",
    "Dallas Mavericks","Denver Nuggets","Detroit Pistons","Golden State Warriors","Houston Rockets","Indiana Pacers",
    "Los Angeles Clippers","Los Angeles Lakers","Memphis Grizzlies","Miami Heat","Milwaukee Bucks","Minnesota Timberwolves",
    "New Orleans Pelicans","New York Knicks","Oklahoma City Thunder","Orlando Magic","Philadelphia 76ers","Phoenix Suns",
    "Portland Trail Blazers","Sacramento Kings","San Antonio Spurs","Toronto Raptors","Utah Jazz","Washington Wizards"
]}

# ──────────────────────────────────────────────
# ODDS HELPERS
# ──────────────────────────────────────────────
def cast(v):
    if v in (None, "", 0): return 0
    if isinstance(v, (int, float)): return int(v)
    m = re.search(r"[-+]?\d+", str(v))
    return int(m.group()) if m else 0

def american_to_prob(odds):
    return 100/(odds+100) if odds>0 else abs(odds)/(abs(odds)+100) if odds else 0

@st.cache_data(ttl=3600)
def load_all_odds(start: str, end: str) -> pd.DataFrame:
    frames = []
    names = [alias for alias in TEAM_ALIAS.values()]
    placeholders = ",".join(["%s"]*len(names))
    for (et, el), tbl in FUTURES_TABLE.items():
        q = f"SELECT team_name,date_created,{','.join(sportsbooks)} FROM {tbl} WHERE team_name IN ({placeholders}) AND date_created BETWEEN %s AND %s ORDER BY team_name,date_created"
        with new_conn(FUTURES_CFG) as conn, conn.cursor() as cur:
            cur.execute(q, (*names, start, end))
            raw = pd.DataFrame(cur.fetchall())
        if raw.empty: continue
        raw[sportsbooks] = raw[sportsbooks].apply(pd.to_numeric, errors='coerce').fillna(0)
        raw['best'] = raw[sportsbooks].replace(0, np.nan).max(axis=1).fillna(0).astype(int)
        raw['prob'] = raw['best'].apply(american_to_prob)
        raw['date'] = pd.to_datetime(raw['date_created']).dt.normalize()
        snap = raw.sort_values(['team_name','date']).groupby(['team_name','date']).tail(1)
        snap['EventType'], snap['EventLabel'] = et, el
        frames.append(snap[['team_name','date','prob','EventType','EventLabel']])
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

# ──────────────────────────────────────────────
# PAGE PARAMETERS
# ──────────────────────────────────────────────
col1, col2 = st.columns(2)
start_date = col1.date_input("Start", datetime.utcnow().date() - timedelta(days=180))
end_date   = col2.date_input("End",   datetime.utcnow().date())
if start_date > end_date:
    st.error("Start date must precede end date."); st.stop()

if not st.button("Show EV Trend"):
    st.stop()

# ──────────────────────────────────────────────
# LOAD BETS
# ──────────────────────────────────────────────
with new_conn(BETTING_CFG) as conn, conn.cursor() as cur:
    cur.execute("""
        SELECT b.WagerID,b.PotentialPayout,b.DollarsAtStake,b.DateTimePlaced,b.WLCA,
               l.EventType,l.EventLabel,l.ParticipantName
          FROM bets b JOIN legs l ON b.WagerID=l.WagerID
         WHERE b.WhichBankroll='GreenAleph' AND l.LeagueName='NBA'
    """ )
    bets = cur.fetchall()
bets_df = pd.DataFrame(bets)
if bets_df.empty:
    st.warning("No NBA wagers found."); st.stop()

# Convert numeric columns to appropriate types
if not bets_df.empty:
    try:
        bets_df['PotentialPayout'] = pd.to_numeric(bets_df['PotentialPayout'], errors='coerce')
        bets_df['DollarsAtStake'] = pd.to_numeric(bets_df['DollarsAtStake'], errors='coerce')
        bets_df['DateTimePlaced'] = pd.to_datetime(bets_df['DateTimePlaced'], errors='coerce')
        # Replace NaN values with 0 for numeric columns
        bets_df['PotentialPayout'].fillna(0, inplace=True)
        bets_df['DollarsAtStake'].fillna(0, inplace=True)
    except Exception as e:
        st.error(f"Error converting data types: {e}")
        st.stop()

# ──────────────────────────────────────────────
# PRELOAD ODDS SNAPSHOTS
# ──────────────────────────────────────────────
odds_df = load_all_odds(f"{start_date} 00:00:00", f"{end_date} 23:59:59")
if odds_df.empty:
    st.warning("No odds data for that period."); st.stop()

# ──────────────────────────────────────────────
# WEEKLY EV CALCULATION
# ──────────────────────────────────────────────
weekly_dates = pd.date_range(start=start_date, end=end_date, freq='7D')
portfolio_ev = []
for dt in weekly_dates:
    dt_norm = dt.normalize()
    # Make sure DateTimePlaced is datetime and handle comparison safely
    subset = bets_df[
        (pd.notna(bets_df['DateTimePlaced'])) & 
        (bets_df['DateTimePlaced'] <= dt_norm) & 
        (bets_df['WLCA']=='Active')
    ]
    ev = 0.0
    for _, grp in subset.groupby('WagerID'):
        pot   = grp['PotentialPayout'].iloc[0]
        stake = grp['DollarsAtStake'].iloc[0]
        probs, decs = [], []
        for _, leg in grp.iterrows():
            sel = odds_df[(odds_df['team_name']==TEAM_ALIAS.get(leg['ParticipantName'], leg['ParticipantName']))
                         & (odds_df['EventType']==leg['EventType'])
                         & (odds_df['EventLabel']==leg['EventLabel'])
                         & (odds_df['date']<=dt_norm)]
            if sel.empty:
                probs=[]; break
            p = sel['prob'].iloc[-1]
            probs.append(p)
            decs.append(1/p if p else 1)
        if not probs:
            continue
        # Handle potential None or invalid values
        if not probs or None in (pot, stake) or not all(isinstance(p, (int, float)) for p in probs):
            continue
            
        try:
            prob_prod = np.prod(probs)
            expected = float(pot) * prob_prod if prob_prod else 0
            denom = sum(d-1 for d in decs)
            if denom <= 0: 
                continue
                
            for d in decs:
                w = (d-1)/denom
                ev += w*(expected - float(stake))
        except (TypeError, ValueError) as e:
            st.error(f"Calculation error: {e}")
            continue
    portfolio_ev.append({'date': dt_norm.to_pydatetime(), 'EV': ev})
trend = pd.DataFrame(portfolio_ev)
if trend.empty:
    st.warning("Could not compute EV trend."); st.stop()

# ──────────────────────────────────────────────
# PLOT
# ──────────────────────────────────────────────
fig, ax = plt.subplots(figsize=(11,5))
ax.plot(trend['date'], trend['EV'], marker='o', linewidth=2)
ax.axhline(0, color='gray', linestyle='--', linewidth=1)
ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
ax.set_ylabel('Expected Value ($)')
ax.set_title('Portfolio EV (Weekly Snapshots)')
plt.xticks(rotation=45)
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
st.pyplot(fig, use_container_width=True)
