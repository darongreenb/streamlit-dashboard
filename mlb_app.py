import streamlit as st
import pymysql
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta
import re

# ───────────────── DB HELPERS ─────────────────
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
    conn.ping(reconnect=True)
    return conn.cursor()

# ───────────────── UTILITIES ─────────────────
def american_odds_to_prob(o):
    return 100 / (o + 100) if o > 0 else abs(o) / (abs(o) + 100) if o else 0.0

def cast_odds(v):
    if v in (None, '', 0): return 0
    if isinstance(v, (int, float)): return int(v)
    m = re.search(r"[-+]?\d+", str(v))
    return int(m.group()) if m else 0

futures_table_map = {
    ("Most Valuable Player Award", "Award"): "NBAMVP",
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
    ("Rookie of Year Award", "Award"): "NBARotY",
    ("Sixth Man of Year Award", "Award"): "NBASixthMotY",
}

# ───────────────── STREAMLIT PAGE ─────────────────
def return_plot_page():
    st.header("% Return Plot (Weekly, Dynamic Stake + Net Return)")

    fut_conn = new_futures_conn()
    bet_conn = new_betting_conn()

    ev_types = sorted({t for (t, _) in futures_table_map})
    sel_type = st.selectbox("Event Type", ev_types)
    labels = sorted({lbl for (t, lbl) in futures_table_map if t == sel_type})
    sel_lbl = st.selectbox("Event Label", labels)

    col1, col2 = st.columns(2)
    start_date = col1.date_input("Start Date", datetime.utcnow().date() - timedelta(days=90))
    end_date = col2.date_input("End Date", datetime.utcnow().date())
    if start_date > end_date:
        st.error("Start date must be before end date.")
        return

    if not st.button("Generate Plot"):
        st.stop()

    tbl_name = futures_table_map.get((sel_type, sel_lbl))
    if not tbl_name:
        st.error("Invalid event type/label combination.")
        return

    # Load all relevant bets
    with with_cursor(bet_conn) as cur:
        cur.execute("""
            SELECT b.WagerID, b.PotentialPayout, b.DollarsAtStake, b.DateTimePlaced, b.WLCA, b.NetProfit,
                   l.EventType, l.EventLabel, l.ParticipantName
            FROM bets b
            JOIN legs l ON b.WagerID = l.WagerID
            WHERE b.WhichBankroll='GreenAleph' AND l.LeagueName='NBA'
                  AND l.EventType=%s AND l.EventLabel=%s
        """, (sel_type, sel_lbl))
        rows = cur.fetchall()
    df = pd.DataFrame(rows)
    if df.empty:
        st.warning("No relevant bets found.")
        return

    df = df[df["DateTimePlaced"].notnull()].copy()
    df["DateTimePlaced"] = pd.to_datetime(df["DateTimePlaced"])
    df = df[df["DateTimePlaced"].dt.date <= end_date]
    df["week"] = df["DateTimePlaced"].dt.to_period("W").dt.start_time

    # Separate active vs realized components
    active_df = df[df["WLCA"] == "Active"].copy()
    real_df   = df[df["WLCA"].isin(["Win", "Loss", "Cashout"])]

    # Aggregate by week
    active_w = active_df.groupby("week").agg({
        "DollarsAtStake": "sum",
        "PotentialPayout": "sum"
    }).rename(columns={
        "DollarsAtStake": "stake",
        "PotentialPayout": "payout"
    })
    real_w = real_df.groupby("week")["NetProfit"].sum().rename("realized_net")

    # Combine and fill missing
    weekly = pd.DataFrame(index=pd.date_range(start=start_date, end=end_date, freq="W-MON"))
    weekly.index.name = "week"
    weekly = weekly.join(active_w, how="left").join(real_w, how="left").fillna(0)

    weekly["cum_stake"] = weekly["stake"].cumsum()
    weekly["cum_payout"] = weekly["payout"].cumsum()
    weekly["cum_realized"] = weekly["realized_net"].cumsum()
    weekly["expected_value"] = weekly["cum_payout"] + weekly["cum_realized"] - weekly["cum_stake"]
    weekly["pct_return"] = (weekly["expected_value"] / weekly["cum_stake"]).replace([float("inf"), -float("inf")], 0.0) * 100

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(weekly.index, weekly["pct_return"], marker="o")
    ax.set_title(f"% Return — {sel_type}: {sel_lbl}")
    ax.set_xlabel("Week")
    ax.set_ylabel("Return (%)")
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
    plt.xticks(rotation=45)
    st.pyplot(fig, use_container_width=True)

# ───────────────── SIDEBAR NAV ─────────────────
page = st.sidebar.radio("Choose Page", ["% Return Plot"])
if page == "% Return Plot":
    return_plot_page()
