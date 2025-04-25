# ─────────────────────  NBA Futures Dashboard  ──────────────────────
import streamlit as st
import pymysql, re, math
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
    conn.ping(reconnect=True)
    return conn.cursor()

# ─────────────────  ODDS HELPERS  ────────────────
def american_odds_to_decimal(o:int)->float:
    if o == 0: return 1.0
    return 1.0 + (o/100) if o>0 else 1.0 + 100/abs(o)
def american_odds_to_prob(o:int)->float:
    if o == 0: return 0.0
    return 100/(o+100) if o>0 else abs(o)/(abs(o)+100)
def cast_odds(v):
    if v in (None,"",0): return 0
    if isinstance(v,(int,float)): return int(v)
    m=re.search(r"[-+]?\d+",str(v)); return int(m.group()) if m else 0

# ─────────────────  MAPS  ────────────────────────
futures_table_map = {
    ("Championship","NBA Championship"):           "NBAChampionship",
    ("Conference Winner","Eastern Conference"):    "NBAEasternConference",
    ("Conference Winner","Western Conference"):    "NBAWesternConference",
    ("Defensive Player of Year Award","Award"):    "NBADefensivePotY",
    ("Division Winner","Atlantic Division"):       "NBAAtlantic",
    ("Division Winner","Central Division"):        "NBACentral",
    ("Division Winner","Northwest Division"):      "NBANorthwest",
    ("Division Winner","Pacific Division"):        "NBAPacific",
    ("Division Winner","Southeast Division"):      "NBASoutheast",
    ("Division Winner","Southwest Division"):      "NBASouthwest",
    ("Most Improved Player Award","Award"):        "NBAMIP",
    ("Most Valuable Player Award","Award"):        "NBAMVP",
    ("Rookie of Year Award","Award"):              "NBARotY",
    ("Sixth Man of Year Award","Award"):           "NBASixthMotY",
}

team_alias_map = {
    #    full name ➜ alias in the futures DB
    **{t:t.split()[-1] for t in [   # quick default: last word
        "Denver Nuggets","Boston Celtics","Miami Heat","Los Angeles Lakers",
        "Golden State Warriors","Charlotte Hornets"
    ]},
    "Philadelphia 76ers":"76ers","Milwaukee Bucks":"Bucks",
    "Los Angeles Clippers":"Clippers","Memphis Grizzlies":"Grizzlies",
    "New Orleans Pelicans":"Pelicans","Portland Trail Blazers":"Trail Blazers",
    # ... add more if you see alias warnings
}

sportsbook_cols = ["BetMGM","DraftKings","Caesars","ESPNBet",
                   "FanDuel","BallyBet","RiversCasino","Bet365"]

# ════════════════════════════════════════════════════════════════════════
# Get best odds ≤ cutoff_dt, but keep scanning back (max 30 days) until
# we hit a row that has at least one non-zero sportsbook column.
# ════════════════════════════════════════════════════════════════════════
def best_odds_decimal_prob(event_type,event_label,participant,cutoff_dt,fut_conn,max_back=30):
    tbl = futures_table_map.get((event_type,event_label))
    if not tbl: return 1.0, 0.0, False
    alias = team_alias_map.get(participant,participant)

    with with_cursor(fut_conn) as cur:
        for delta in range(max_back+1):
            d = cutoff_dt - timedelta(days=delta)
            cur.execute(
                f"""SELECT {','.join(sportsbook_cols)}
                      FROM {tbl}
                     WHERE team_name=%s AND DATE(date_created)=DATE(%s)
                     ORDER BY date_created DESC LIMIT 1""",
                (alias, d)
            )
            row = cur.fetchone()
            if not row: continue
            nums=[cast_odds(row[c]) for c in sportsbook_cols if cast_odds(row[c])!=0]
            if nums:
                best=max(nums)
                return american_odds_to_decimal(best), american_odds_to_prob(best), True
        return 1.0,0.0,False   # never found

# ─────────────────────────  EV TABLE PAGE  ────────────────────────────
def ev_table_page():
    st.header("EV Table")
    bet_conn = new_betting_conn()
    fut_conn = new_futures_conn()
    now      = datetime.utcnow()

    # -- Active wagers -------------------------------------------------
    q_active = """
        SELECT b.WagerID,b.PotentialPayout,b.DollarsAtStake,
               l.EventType,l.EventLabel,l.ParticipantName
          FROM bets b JOIN legs l ON b.WagerID=l.WagerID
         WHERE b.WhichBankroll='GreenAleph' AND b.WLCA='Active' AND l.LeagueName='NBA'
    """
    with with_cursor(bet_conn) as cur: cur.execute(q_active); act_rows=cur.fetchall()
    active = defaultdict(lambda:{"pot":0,"stake":0,"legs":[]})
    for r in act_rows:
        w=active[r["WagerID"]]
        w["pot"]=w["pot"] or float(r["PotentialPayout"] or 0)
        w["stake"]=w["stake"] or float(r["DollarsAtStake"] or 0)
        w["legs"].append((r["EventType"],r["EventLabel"],r["ParticipantName"]))

    active_stake,active_exp = defaultdict(float),defaultdict(float)
    for data in active.values():
        pot,stake,legs = data["pot"],data["stake"],data["legs"]
        parts=[]; parlay_prob=1
        for et,el,pn in legs:
            dec,p,ok = best_odds_decimal_prob(et,el,pn,now,fut_conn)
            if not ok: parlay_prob=0; break
            parts.append((dec,et,el)); parlay_prob*=p
        if parlay_prob==0: continue
        expected=pot*parlay_prob
        sum_exc=sum(d-1 for d,_,_ in parts);  # weights
        if sum_exc<=0: continue
        for d,et,el in parts:
            w=(d-1)/sum_exc
            active_stake[(et,el)]+=w*stake
            active_exp  [(et,el)]+=w*expected

    # -- Realised wager nets -------------------------------------------
    q_real = """
        SELECT b.WagerID,b.NetProfit,
               l.EventType,l.EventLabel,l.ParticipantName
          FROM bets b JOIN legs l ON b.WagerID=l.WagerID
         WHERE b.WhichBankroll='GreenAleph'
           AND b.WLCA IN ('Win','Loss','Cashout')
           AND l.LeagueName='NBA'
    """
    with with_cursor(bet_conn) as cur: cur.execute(q_real); real_rows=cur.fetchall()
    bet_conn.close(); fut_conn.close()

    wager_net=defaultdict(float); wager_legs=defaultdict(list)
    for r in real_rows:
        wager_net[r["WagerID"]]=float(r["NetProfit"] or 0)
        wager_legs[r["WagerID"]].append((r["EventType"],r["EventLabel"],r["ParticipantName"]))

    fut_conn=new_futures_conn()   # reopen for lookups
    realized_np=defaultdict(float)
    for wid,legs in wager_legs.items():
        net=wager_net[wid]
        decs=[(best_odds_decimal_prob(et,el,pn,now,fut_conn)[0],et,el) for et,el,pn in legs]
        sum_exc=sum(d-1 for d,_,_ in decs)
        if sum_exc<=0: continue
        for d,et,el in decs:
            realized_np[(et,el)]+=net*((d-1)/sum_exc)
    fut_conn.close()

    # -- Build DataFrame -----------------------------------------------
    keys=set(active_stake)|set(active_exp)|set(realized_np)
    rows=[]
    for (et,el) in sorted(keys):
        stake=active_stake.get((et,el),0)
        exp  =active_exp  .get((et,el),0)
        net  =realized_np .get((et,el),0)
        rows.append(dict(EventType=et,EventLabel=el,
                         ActiveDollarsAtStake=round(stake,2),
                         ActiveExpectedPayout=round(exp,2),
                         RealizedNetProfit   =round(net,2),
                         ExpectedValue       =round(exp-stake+net,2)))
    st.dataframe(pd.DataFrame(rows)
                 .sort_values(["EventType","EventLabel"])
                 .reset_index(drop=True),
                 use_container_width=True)

# ─────────────────────  % RETURN PLOT PAGE  ───────────────────────────
def return_plot_page_fast():
    st.header("% Return Plot (active wagers)")

    fut_conn = new_futures_conn()
    bet_conn = new_betting_conn()

    ev_types = sorted({t for (t,_) in futures_table_map})
    sel_type = st.selectbox("Event Type", ev_types)
    labels   = sorted({l for (t,l) in futures_table_map if t==sel_type})
    sel_lbl  = st.selectbox("Event Label", labels)

    col1,col2=st.columns(2)
    start_d  = col1.date_input("Start", datetime.utcnow().date()-timedelta(days=60))
    end_d    = col2.date_input("End",   datetime.utcnow().date())
    if start_d>end_d:
        st.error("Start date must precede end date"); return

    run = st.button("Generate Plot")
    if not run: return

    # -------- Load active wagers once ----------------
    with with_cursor(bet_conn) as cur:
        cur.execute("""
            SELECT b.WagerID,b.PotentialPayout,b.DollarsAtStake,b.DateTimePlaced,
                   l.EventType,l.EventLabel,l.ParticipantName
              FROM bets b JOIN legs l ON b.WagerID=l.WagerID
             WHERE b.WhichBankroll='GreenAleph' AND b.WLCA='Active' AND l.LeagueName='NBA'
        """)
        raw = pd.DataFrame(cur.fetchall())
    if raw.empty:
        st.warning("No active wagers"); return

    # keep only wagers containing the selected market leg
    raw["is_sel"] = (raw["EventType"]==sel_type)&(raw["EventLabel"]==sel_lbl)
    raw = raw.groupby("WagerID").filter(lambda g: g["is_sel"].any())
    if raw.empty:
        st.warning("No active wagers with that market"); return
    st.info(f"{raw['WagerID'].nunique()} active wagers include that market.")

    # weight (1 / #legs in wager)
    leg_counts = raw.groupby("WagerID").size()
    raw = raw.merge(leg_counts.rename("leg_cnt"), on="WagerID")
    raw["weight"] = 1.0 / raw["leg_cnt"]

    # participants to fetch odds for
    team_names = raw["ParticipantName"].map(lambda x: team_alias_map.get(x,x)).unique().tolist()
    tbl_name   = futures_table_map[(sel_type,sel_lbl)]

    @st.cache_data(ttl=1800, show_spinner=False)
    def fetch_odds(tbl, names, s, e):
        if not names: return pd.DataFrame()
        fmt_s, fmt_e = f"{s} 00:00:00", f"{e} 23:59:59"
        placeholders = ",".join(["%s"]*len(names))
        with with_cursor(fut_conn) as cur:
            cur.execute(
                f"""SELECT team_name,date_created,{','.join(sportsbook_cols)}
                      FROM {tbl}
                     WHERE team_name IN ({placeholders})
                       AND date_created BETWEEN %s AND %s
                     ORDER BY team_name,date_created""",
                (*names, fmt_s, fmt_e)
            )
            df = pd.DataFrame(cur.fetchall())
        if df.empty: return df

        df[sportsbook_cols] = df[sportsbook_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
        df["best"]  = df[sportsbook_cols].mask(df[sportsbook_cols]==0).max(axis=1).fillna(0).astype(int)
        df["prob"]  = df["best"].apply(american_odds_to_prob)
        df["date"]  = pd.to_datetime(df["date_created"]).dt.date
        df = (df.sort_values(["team_name","date"])
                 .groupby(["team_name","date"]).tail(1))[["team_name","date","prob"]]
        return df.reset_index(drop=True)

    odds_df = fetch_odds(tbl_name, team_names, start_d, end_d)
    if odds_df.empty:
        st.warning("Odds table has no data in that window."); return

    # merge odds into raw legs
    raw["alias"] = raw["ParticipantName"].map(lambda x: team_alias_map.get(x,x))
    merged = raw.merge(odds_df, left_on=["alias"], right_on=["team_name"], how="inner")
    merged = merged[merged["date"].between(start_d,end_d)]

    if merged.empty:
        st.warning("No intersecting odds snapshots"); return

    # compute daily contribution per wager
    merged["exp"]   = merged["PotentialPayout"] * merged["prob"] * merged["weight"]
    merged["stake"] = merged["DollarsAtStake"] * merged["weight"]
    merged["net"]   = merged["exp"] - merged["stake"]

    daily = (merged.groupby(["date"])
                   .agg(net=("net","sum"), stake=("stake","sum"))
                   .reset_index())
    daily["pct"] = (daily["net"] / daily["stake"]) * 100

    if daily.empty:
        st.warning("No data survives final aggregation."); return

    # -------- PLOT -------------
    fig, ax = plt.subplots(figsize=(10,5))
    ax.plot(daily["date"], daily["pct"], marker="o")
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
    ax.set_title(f"% Return — {sel_type}: {sel_lbl}")
    ax.set_xlabel("Date"); ax.set_ylabel("Return (%)")
    plt.xticks(rotation=45)
    st.pyplot(fig,use_container_width=True)

# ─────────────────────────  NAVIGATION  ────────────────────────────
page = st.sidebar.radio("Choose Page", ["EV Table", "% Return Plot"])
if page == "EV Table":
    ev_table_page()
else:
    return_plot_page_fast()
