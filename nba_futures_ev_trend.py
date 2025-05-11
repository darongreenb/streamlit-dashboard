# streamlit_app.py  ────────────────────────────────────────────────────────────
import streamlit as st
import pandas as pd, pymysql, re
from collections import defaultdict
from datetime import datetime, date
import plotly.express as px
from typing import Dict, Tuple

# ──────────────────  CONFIG  ──────────────────
st.set_page_config(page_title="Futures EV Dashboard", layout="wide")
st.title("📊 Futures Expected‑Value Dashboard")

# ──────────────────  DB HELPERS  ──────────────────
def conn(section: str):
    """Connect using credentials stored in st.secrets[section]."""
    s = st.secrets[section]
    return pymysql.connect(
        host=s.host, user=s.user, password=s.password,
        database=s.database, autocommit=True,
        cursorclass=pymysql.cursors.DictCursor, connect_timeout=10
    )

def with_cursor(cnx):
    cnx.ping(reconnect=True);  return cnx.cursor()

# helper to run SELECT → DataFrame quickly
def read_sql(sql: str, cnx, **kw):
    return pd.read_sql(sql, cnx, **kw)

# ──────────────────  CONSTANTS  ──────────────────
sportsbook_cols = ["BetMGM","DraftKings","Caesars","ESPNBet",
                   "FanDuel","BallyBet","RiversCasino","Bet365"]

futures_table_map: Dict[Tuple[str,str], str] = {
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

team_alias_map = {"Philadelphia 76ers":"76ers", "Milwaukee Bucks":"Bucks",  # … trimmed
                  "Washington Wizards":"Wizards"}

# ──────────────────  ODDS UTILS  ──────────────────
def am_odds_to_dec(o): return 1 + (o/100) if o>0 else 1 + 100/abs(o) if o else 1
def am_odds_to_prob(o): return 100/(o+100) if o>0 else abs(o)/(abs(o)+100) if o else 0
def cast_odds(v):
    if v in (None,"",0): return 0
    if isinstance(v,(int,float)): return int(v)
    m=re.search(r"[-+]?\d+",str(v)); return int(m.group()) if m else 0

# ──────────────────  BEST‑ODDS LOOK‑BACK  ──────────────────
def best_odds_decimal_prob(event_type, event_label, participant,
                           cutoff_dt, fut_cnx, vig=0.05):
    tbl = futures_table_map.get((event_type, event_label))
    if not tbl: return 1.0, 0.0
    alias = team_alias_map.get(participant, participant)
    cur = with_cursor(fut_cnx)
    cur.execute(
        f"""SELECT {','.join(sportsbook_cols)}
               FROM {tbl}
              WHERE team_name=%s AND date_created<=%s
          ORDER BY date_created DESC LIMIT 100""",
        (alias, cutoff_dt))
    rows = cur.fetchall(); cur.close()
    for r in rows:
        quotes=[cast_odds(r[c]) for c in sportsbook_cols if cast_odds(r[c])]
        if not quotes: continue
        best=min(quotes, key=am_odds_to_prob)          # longest price
        return am_odds_to_dec(best), am_odds_to_prob(best)*(1-vig)
    return 1.0, 0.0

# ──────────────────  BUILD EV TABLE  ──────────────────
def build_ev_table() -> pd.DataFrame:
    bet_cnx, fut_cnx = conn("BETTING_DB"), conn("FUTURES_DB")
    now, vig = datetime.utcnow(), 0.05

    cur = with_cursor(bet_cnx)

    # Active NBA bets ---------------------------------------------------------
    cur.execute("""
        SELECT b.WagerID, b.PotentialPayout, b.DollarsAtStake,
               l.EventType, l.EventLabel, l.ParticipantName
        FROM bets b JOIN legs l ON b.WagerID=l.WagerID
        WHERE b.WhichBankroll='GreenAleph' AND b.WLCA='Active'
          AND l.LeagueName='NBA'""")
    rows = cur.fetchall()
    active=defaultdict(lambda:{"pot":0,"stake":0,"legs":[]})
    for r in rows:
        w=active[r["WagerID"]]
        w["pot"]=w["pot"] or float(r["PotentialPayout"] or 0)
        w["stake"]=w["stake"] or float(r["DollarsAtStake"] or 0)
        w["legs"].append((r["EventType"],r["EventLabel"],r["ParticipantName"]))

    act_stake, act_exp = defaultdict(float), defaultdict(float)
    for data in active.values():
        pot, stake, legs = data["pot"], data["stake"], data["legs"]
        decs, prob = [], 1.0
        for et,el,pn in legs:
            dec,p=best_odds_decimal_prob(et,el,pn,now,fut_cnx,vig)
            if p==0: prob=0; break
            decs.append(dec); prob*=p
        if prob==0: continue
        expected=pot*prob
        exc=sum(d-1 for d in decs);  # excess over 1
        if exc<=0: continue
        for d,(et,el,_) in zip(decs,legs):
            w=(d-1)/exc
            act_stake[(et,el)]+=w*stake
            act_exp[(et,el)]  +=w*expected

    # Realised NBA ------------------------------------------------------------
    cur.execute("""
        SELECT b.WagerID,b.NetProfit,
               l.EventType,l.EventLabel,l.ParticipantName
        FROM bets b JOIN legs l ON b.WagerID=l.WagerID
        WHERE b.WhichBankroll='GreenAleph' AND b.WLCA IN ('Win','Loss','Cashout')
          AND l.LeagueName='NBA'""")
    rows=cur.fetchall()
    wager_net=defaultdict(float); wager_legs=defaultdict(list)
    for r in rows:
        wager_net[r["WagerID"]]=float(r["NetProfit"] or 0)
        wager_legs[r["WagerID"]].append((r["EventType"],r["EventLabel"],r["ParticipantName"]))
    realized=defaultdict(float)
    for wid,legs in wager_legs.items():
        net=wager_net[wid]
        decs=[best_odds_decimal_prob(et,el,pn,now,fut_cnx,vig)[0] for et,el,pn in legs]
        exc=sum(d-1 for d in decs)
        if exc<=0: continue
        for d,(et,el,_) in zip(decs,legs):
            realized[(et,el)]+=net*((d-1)/exc)

    # Completed non‑NBA -------------------------------------------------------
    cur.execute("""
        SELECT b.NetProfit,l.EventType,l.EventLabel,l.LeagueName
        FROM bets b JOIN legs l ON b.WagerID=l.WagerID
        WHERE b.WhichBankroll='GreenAleph'
          AND b.WLCA IN ('Win','Loss','Cashout')
          AND l.LeagueName<>'NBA'""")
    rows=cur.fetchall()
    other=defaultdict(float)
    for r in rows:
        key=(r["LeagueName"],r["EventType"],r["EventLabel"])
        other[key]+=float(r["NetProfit"] or 0)

    # Wallet‑wide realised NP -------------------------------------------------
    cur.execute("SELECT NetProfit FROM bets WHERE WhichBankroll='GreenAleph'")
    total_net=sum(float(r["NetProfit"] or 0) for r in cur.fetchall())
    cur.close(); bet_cnx.close(); fut_cnx.close()

    # Build dataframe ---------------------------------------------------------
    rec=[]
    for et,el in futures_table_map:
        stake=act_stake[(et,el)]; exp=act_exp[(et,el)]; net=realized[(et,el)]
        rec.append(dict(LeagueName="NBA",EventType=et,EventLabel=el,
                        ActiveDollarsAtStake=round(stake,2),
                        ActiveExpectedPayout=round(exp,2),
                        RealizedNetProfit=round(net,2),
                        ExpectedValue=round(exp-stake+net,2)))
    for (lg,et,el),net in other.items():
        rec.append(dict(LeagueName=lg,EventType=et,EventLabel=el,
                        ActiveDollarsAtStake=0,ActiveExpectedPayout=0,
                        RealizedNetProfit=round(net,2),ExpectedValue=round(net,2)))

    df=pd.DataFrame(rec).sort_values(["LeagueName","EventType","EventLabel"]).reset_index(drop=True)

    total_row=dict(LeagueName="TOTAL",EventType="",EventLabel="",
                   ActiveDollarsAtStake=df["ActiveDollarsAtStake"].sum(),
                   ActiveExpectedPayout=df["ActiveExpectedPayout"].sum(),
                   RealizedNetProfit=round(total_net,2),
                   ExpectedValue=round(df["ActiveExpectedPayout"].sum()
                                      -df["ActiveDollarsAtStake"].sum()
                                      +total_net,2))
    return pd.concat([df,pd.DataFrame([total_row])], ignore_index=True)

# ──────────────────  SNAPSHOT HELPERS  ──────────────────
def save_snapshot(total_ev: float):
    cnx=conn("FUTURES_DB"); cur=with_cursor(cnx)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ev_history (
            snapshot_date DATE PRIMARY KEY,
            expected_value DOUBLE
        )""")
    cur.execute("REPLACE INTO ev_history VALUES (%s,%s)", (date.today(), total_ev))
    cur.close(); cnx.close()

def load_history() -> pd.DataFrame:
    cnx=conn("FUTURES_DB")
    df=read_sql("SELECT snapshot_date AS date, expected_value AS ev "
                "FROM ev_history ORDER BY snapshot_date", cnx,
                parse_dates=["date"])
    cnx.close(); return df

# ──────────────────  UI  ──────────────────
if st.button("⟳ Add snapshot / refresh chart"):
    df_current=build_ev_table()
    total_ev=float(df_current.loc[df_current["LeagueName"]=="TOTAL","ExpectedValue"].iloc[0])
    save_snapshot(total_ev)
    st.success(f"Snapshot saved for {date.today():%Y‑%m‑%d}:  ${total_ev:,.0f}")
else:
    df_current=build_ev_table()

# ---- Summary / Table ----
st.subheader("Current Expected‑Value table")
metrics_df=df_current[df_current["LeagueName"]!="TOTAL"]
col1,col2,col3,col4=st.columns(4)
col1.metric("Active stake",f"${metrics_df['ActiveDollarsAtStake'].sum():,.0f}")
col2.metric("Expected payout",f"${metrics_df['ActiveExpectedPayout'].sum():,.0f}")
col3.metric("Realized profit",f"${metrics_df['RealizedNetProfit'].sum():,.0f}")
col4.metric("Total EV",f"${metrics_df['ExpectedValue'].sum():,.0f}")
st.dataframe(df_current.style.format({"ActiveDollarsAtStake":"${:,.0f}",
                                      "ActiveExpectedPayout":"${:,.0f}",
                                      "RealizedNetProfit":"${:,.0f}",
                                      "ExpectedValue":"${:,.0f}"}),
             use_container_width=True, height=650)

# ---- EV trend ----
st.subheader("EV trend (manual snapshots)")
hist_df=load_history()
if hist_df.empty():
    st.info("No snapshots yet. Click the button above to record today’s EV.")
else:
    fig=px.line(hist_df,x="date",y="ev",markers=True,
                labels={"date":"Date","ev":"Expected value ($)"})
    st.plotly_chart(fig,use_container_width=True)
    st.dataframe(hist_df.style.format({"ev":"${:,.0f}"}), use_container_width=True)
