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




# ─────────────────────────────────────────────────────────────────────────
#  Fast %‑Return plot page
# ─────────────────────────────────────────────────────────────────────────
def return_plot_page_fast():
    st.header("% Return Plot (optimised)")

    fut_conn = new_futures_conn()
    bet_conn = new_betting_conn()

    # ------------ user filters -----------
    ev_types = sorted({t for (t, _) in futures_table_map})
    sel_type = st.selectbox("Event Type", ev_types)
    labels   = sorted({lbl for (t, lbl) in futures_table_map if t == sel_type})
    sel_lbl  = st.selectbox("Event Label", labels)

    col1, col2 = st.columns(2)
    start_date = col1.date_input("Start",  datetime.utcnow().date() - timedelta(days=60))
    end_date   = col2.date_input("End",    datetime.utcnow().date())
    if start_date > end_date:
        st.error("Start date must precede end date"); return

    if not st.button("Generate Plot"):
        st.info("Choose filters & press **Generate Plot**")
        return

    # ------------ 1) load ACTIVE wagers once -----------
    with with_cursor(bet_conn) as cur:
        cur.execute("""
            SELECT b.WagerID, b.PotentialPayout, b.DollarsAtStake, b.DateTimePlaced,
                   l.EventType, l.EventLabel, l.ParticipantName
              FROM bets b JOIN legs l ON b.WagerID = l.WagerID
             WHERE b.WhichBankroll='GreenAleph' AND b.WLCA='Active' AND l.LeagueName='NBA'
        """)
        bet_rows = cur.fetchall()
    bet_df = pd.DataFrame(bet_rows)

    if bet_df.empty:
        st.warning("No active wagers"); return

    # pre‑compute leg‑weights toward the selected market -------------------
    #  weight = (decimal‑odds‑1) / Σ(decimal‑odds‑1) for that wager
    sel_bets = bet_df.groupby("WagerID")

    leg_weights = []
    for wid, grp in sel_bets:
        dec_all = []
        for _, r in grp.iterrows():
            dec_all.append( (american_odds_to_decimal(100), r["EventType"], r["EventLabel"]) )  # placeholder 100/+100
        # using placeholder for now; we will replace with real odds df merge later
        sum_exc = sum(d-1 for d,_,_ in dec_all)
        if sum_exc <= 0: continue
        for d, et, el in dec_all:
            if (et,el) == (sel_type, sel_lbl):
                leg_weights.append(dict(WagerID=wid, Weight=(d-1)/sum_exc))
    wgt_df = pd.DataFrame(leg_weights)
    if wgt_df.empty:
        st.warning("No legs found for that market in active wagers"); return

    # ------------ 2) BULK‑load futures odds snapshot ---------------------
    participants = bet_df["ParticipantName"].map(lambda x: team_alias_map.get(x, x)).unique().tolist()
    tbl_name     = futures_table_map[(sel_type, sel_lbl)]

    @st.cache_data(ttl=3600, show_spinner=False)
    def cached_odds(tbl, names, start, end):
        fmt = "%Y-%m-%d %H:%M:%S"
        with with_cursor(fut_conn) as cur:
            cur.execute(
                f"""SELECT team_name,date_created,{','.join(sportsbook_cols)}
                      FROM {tbl}
                     WHERE team_name IN ({','.join(['%s']*len(names))})
                       AND date_created BETWEEN %s AND %s
                     ORDER BY team_name,date_created""",
                (*names, f"{start} 00:00:00", f"{end} 23:59:59")
            )
            df = pd.DataFrame(cur.fetchall())
        if df.empty: return df
        # best non‑zero odds across books, then -> decimal & probability
        df["best"] = df[sportsbook_cols].apply(lambda r: max([cast_odds(x) for x in r if cast_odds(x)!=0] or [0]), axis=1)
        df["prob"] = df["best"].apply(american_odds_to_prob)
        df["date"] = pd.to_datetime(df["date_created"]).dt.date
        return df[["team_name","date","prob"]]
    odds_df = cached_odds(tbl_name, participants, start_date, end_date)
    if odds_df.empty:
        st.warning("No odds data for that period"); return

    # ------------ 3) build daily probability lookup ----------------------
    # latest prob for each team on each day
    odds_df.sort_values(["team_name","date"], inplace=True)
    odds_df = odds_df.groupby(["team_name","date"]).tail(1)

    # ------------ 4) combine into daily return ---------------------------
    #   For each wager: stake_i = DollarsAtStake * weight
    #                   exp_i(day) = PotentialPayout * Π(prob_j(day)) * weight
    bet_meta = bet_df[["WagerID","PotentialPayout","DollarsAtStake","DateTimePlaced"]].drop_duplicates()
    merged   = bet_df.merge(odds_df, how="left",
                            left_on=["ParticipantName"],
                            right_on=["team_name"])
    merged["date"] = merged["date"].fillna(method="ffill")   # carry last prob backwards if missing
    merged.dropna(subset=["prob"], inplace=True)

    # multiply probabilities within each wager/day
    merged = merged.merge(wgt_df, on="WagerID", how="inner")          # keep only legs from selected market
    merged["prob_pow"] = merged["prob"]                              # since only 1 leg of interest
    daily = (merged
             .groupby(["date","WagerID"])
             .agg(prob_product=("prob_pow","prod"),
                  weight=("Weight","first"))
             .reset_index())

    daily = daily.merge(bet_meta, on="WagerID", how="left")
    daily["stake_part"] = daily["DollarsAtStake"] * daily["weight"]
    daily["exp_part"]   = daily["PotentialPayout"] * daily["prob_product"] * daily["weight"]
    daily["net_part"]   = daily["exp_part"] - daily["stake_part"]

    series = (daily.groupby("date")
                   .agg(net=("net_part","sum"), stake=("stake_part","sum"))
                   .reset_index())
    series["pct"] = (series["net"] / series["stake"]) * 100
    series = series[series["date"].between(start_date,end_date)]

    # ------------ 5) plot ---------------
    if series.empty.any():
        st.warning("Not enough data to plot"); return
    fig, ax = plt.subplots(figsize=(10,5))
    ax.plot(series["date"], series["pct"], marker="o")
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
    ax.set_title(f"% Return – {sel_type}: {sel_lbl}")
    ax.set_xlabel("Date"); ax.set_ylabel("Return (%)")
    plt.xticks(rotation=45)
    st.pyplot(fig, use_container_width=True)

# ───────────────────────────  SIDEBAR NAV  ─────────────────────────
page = st.sidebar.radio("Choose Page", ["EV Table", "% Return Plot"])
if page == "EV Table":
    ev_table_page()
else:
    return_plot_page_fast()
