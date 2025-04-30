import streamlit as st, pymysql, pandas as pd, matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta

# ─────────── helpers ───────────
def new_betting_conn():
    return pymysql.connect(**st.secrets["BETTING_DB"],
                           cursorclass=pymysql.cursors.DictCursor,
                           autocommit=True)

def with_cursor(c): c.ping(reconnect=True); return c.cursor()

# ─────────── page ───────────
def weekly_expected_profit():
    st.header("Expected Profit – weekly snapshot")

    ev_map = {("Most Valuable Player Award","Award"):None,
              ("Championship","NBA Championship"):None}   # map still unused here
    etype = st.selectbox("Event Type", sorted({t for t,_ in ev_map}))
    elabel= st.selectbox("Event Label",
                         sorted({l for t,l in ev_map if t==etype}))
    sd, ed = st.date_input("Start", datetime.utcnow().date()-timedelta(90)),\
             st.date_input("End",   datetime.utcnow().date())
    if sd>ed: st.error("Start > End"); return
    if not st.button("Plot"): return

    # pull once
    with with_cursor(new_betting_conn()) as cur:
        cur.execute(
        """SELECT b.WagerID, b.PotentialPayout, b.DollarsAtStake, b.NetProfit,
                  b.DateTimePlaced, b.WLCA
             FROM bets b
             JOIN legs l ON b.WagerID=l.WagerID
            WHERE b.WhichBankroll='GreenAleph'
              AND l.LeagueName='NBA'
              AND l.EventType=%s AND l.EventLabel=%s""",
            (etype, elabel))
        df = pd.DataFrame(cur.fetchall())
    if df.empty: st.warning("No bets"); return

    df["DateTimePlaced"]=pd.to_datetime(df["DateTimePlaced"])
    for c in ["PotentialPayout","DollarsAtStake","NetProfit"]:
        df[c]=pd.to_numeric(df[c],errors="coerce").fillna(0)

    weeks = pd.date_range(sd, ed, freq="W-MON")
    out=[]
    for w in weeks:
        active   =(df[(df.WLCA=="Active") & (df.DateTimePlaced<=w)]
                   .drop_duplicates("WagerID"))
        resolved =(df[(df.WLCA.isin(["Win","Loss","Cashout"])) &
                      (df.DateTimePlaced<=w)]
                   .drop_duplicates("WagerID"))
        pot,stake = active.PotentialPayout.sum(), active.DollarsAtStake.sum()
        net = resolved.NetProfit.sum()
        out.append({"week":w, "expected_profit":pot-stake+net})
    plot=pd.DataFrame(out)

    fig,ax=plt.subplots(figsize=(9,5))
    ax.plot(plot.week,plot.expected_profit,marker="o")
    ax.set_title(f"Expected Profit — {etype}: {elabel}")
    ax.set_xlabel("Week"); ax.set_ylabel("$")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    plt.xticks(rotation=45)
    st.pyplot(fig, use_container_width=True)

# ─────────── run ───────────
if st.sidebar.radio("Page",["Weekly Expected Profit"])=="Weekly Expected Profit":
    weekly_expected_profit()
