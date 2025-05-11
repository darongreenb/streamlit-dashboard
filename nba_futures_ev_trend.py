# ─────────────────────────────  Futures Dashboard (one‑file)  ─────────────────────────────
import streamlit as st, pandas as pd, pymysql, re
from collections import defaultdict
from datetime import datetime, date
import plotly.express as px

# -----------------------------------------------------------------------------------------
#                                   ─── CONFIG ───
# -----------------------------------------------------------------------------------------
st.set_page_config(page_title="Futures EV dashboard", layout="wide")
st.title("📈 GreenAleph – Futures Expected Value")

# -----------------------------------------------------------------------------------------
#                         ─── DATABASE CONNECTION HELPERS ───
# -----------------------------------------------------------------------------------------
def conn(section):
    s = st.secrets[section]
    return pymysql.connect(
        host=s.host, user=s.user, password=s.password,
        database=s.database, autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )

# ensure ev_history exists (runs once)
with conn("FUTURES_DB") as c, c.cursor() as cur:
    cur.execute("""CREATE TABLE IF NOT EXISTS ev_history
                   (snapshot_date DATE PRIMARY KEY, expected_value DOUBLE)""")

# -----------------------------------------------------------------------------------------
#                             ─── ODDS + UTILS (unchanged) ───
# -----------------------------------------------------------------------------------------
def american_odds_to_decimal(o): return 1 + (o/100) if o > 0 else 1 + 100/abs(o) if o else 1
def american_odds_to_prob(o):    return 100/(o+100) if o > 0 else abs(o)/(abs(o)+100) if o else 0
def cast_odds(v):
    if v in (None, "", 0): return 0
    if isinstance(v, (int, float)): return int(v)
    m = re.search(r"[-+]?\d+", str(v)); return int(m.group()) if m else 0

# … (futures_table_map, team_alias_map, sportsbook_cols – copy exactly from your last script) …
futures_table_map = {("Championship","NBA Championship"):"NBAChampionship", ...}
team_alias_map    = {"Philadelphia 76ers":"76ers",  ...}
sportsbook_cols   = ["BetMGM","DraftKings","Caesars","ESPNBet","FanDuel",
                     "BallyBet","RiversCasino","Bet365"]

def best_odds_decimal_prob(event_type,event_label,participant,cutoff,fut, vig=0.05):
    tbl=futures_table_map.get((event_type,event_label))
    if not tbl: return 1.0,0.0
    alias=team_alias_map.get(participant,participant)
    with fut.cursor() as cur:
        cur.execute(f"""SELECT {','.join(sportsbook_cols)}
                          FROM {tbl}
                         WHERE team_name=%s AND date_created<=%s
                     ORDER BY date_created DESC LIMIT 60""",
                    (alias,cutoff))
        rows=cur.fetchall()
    for r in rows:
        quotes=[cast_odds(r.get(c)) for c in sportsbook_cols if cast_odds(r.get(c))]
        if not quotes: continue
        best=min(quotes,key=american_odds_to_prob)        # longest price
        return (d:=american_odds_to_decimal(best)), american_odds_to_prob(best)*(1-vig)
    return 1.0,0.0

# -----------------------------------------------------------------------------------------
#                       ─── MAIN EV‑TABLE CONSTRUCTION ● SAME LOGIC ───
# -----------------------------------------------------------------------------------------
@st.cache_data(show_spinner=False)
def build_ev_table():
    bet, fut = conn("BETTING_DB"), conn("FUTURES_DB")
    now, vig = datetime.utcnow(), {k:0.05 for k in futures_table_map}

    with bet.cursor() as cur:
        # Active NBA legs
        cur.execute("""SELECT b.WagerID,b.PotentialPayout,b.DollarsAtStake,
                              l.EventType,l.EventLabel,l.ParticipantName
                         FROM bets b JOIN legs l ON b.WagerID=l.WagerID
                        WHERE b.WhichBankroll='GreenAleph' AND b.WLCA='Active'
                          AND l.LeagueName='NBA'""")
        active_rows = cur.fetchall()

        # Settled NBA legs
        cur.execute("""SELECT b.WagerID,b.NetProfit,
                              l.EventType,l.EventLabel,l.ParticipantName
                         FROM bets b JOIN legs l ON b.WagerID=l.WagerID
                        WHERE b.WhichBankroll='GreenAleph'
                          AND b.WLCA IN ('Win','Loss','Cashout')
                          AND l.LeagueName='NBA'""")
        settled_rows = cur.fetchall()

        # All settled non‑NBA legs
        cur.execute("""SELECT b.NetProfit,l.EventType,l.EventLabel,l.LeagueName
                         FROM bets b JOIN legs l ON b.WagerID=l.WagerID
                        WHERE b.WhichBankroll='GreenAleph'
                          AND b.WLCA IN ('Win','Loss','Cashout')
                          AND l.LeagueName<>'NBA'""")
        nonnba_rows = cur.fetchall()

        # Wallet‑wide net profit (TOTAL row rule)
        cur.execute("SELECT NetProfit FROM bets WHERE WhichBankroll='GreenAleph'")
        total_net = sum(float(r["NetProfit"] or 0) for r in cur.fetchall())

    # … identical aggregation code as before (active_stake, active_exp, realized_np, etc.)
    # (copy your latest, making sure the ‘order‑bug’ fixes are present)

    # return final df
    return df, total_net

# -----------------------------------------------------------------------------------------
#                             ─── 1) DISPLAY EV TABLE ───
# -----------------------------------------------------------------------------------------
df, wallet_np = build_ev_table()
st.subheader("Market table")
# (use your existing display_data() or quick st.dataframe(df))

# -----------------------------------------------------------------------------------------
#                   ─── 2)  ADD / VIEW EV‑HISTORY SNAPSHOTS ───
# -----------------------------------------------------------------------------------------
today = date.today()
if st.button("➕ Add latest snapshot"):
    # check if today already exists → replace, else insert
    with conn("FUTURES_DB") as c, c.cursor() as cur:
        cur.execute("""REPLACE INTO ev_history
                       (snapshot_date, expected_value)
                       VALUES (%s, %s)""",
                    (today, float(df.loc[df["LeagueName"]=="TOTAL","ExpectedValue"].iloc[0])))
    st.success("Snapshot saved!")

# load history for plot
hist = pd.read_sql("SELECT snapshot_date AS date, expected_value AS ev "
                   "FROM ev_history ORDER BY date",
                   conn("FUTURES_DB"), parse_dates=["date"])

st.subheader("📈 Portfolio EV over time")
if hist.empty:
    st.info("No snapshots yet. Press the button above to create the first one.")
else:
    fig = px.line(hist, x="date", y="ev", markers=True,
                  labels={"date":"Date", "ev":"Expected value ($)"})
    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(hist.style.format({"ev":"${:,.0f}"}), use_container_width=True)

# -----------------------------------------------------------------------------------------
# ℹ️  The build_ev_table() result is cached for 10 minutes by @st.cache_data – press
#     ⟳ (“Rerun”) in Streamlit if you need a fresh calculation before the cache expires.
# -----------------------------------------------------------------------------------------
