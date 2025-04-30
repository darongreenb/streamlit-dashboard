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

def with_cursor(conn):
    conn.ping(reconnect=True)
    return conn.cursor()

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
def expected_profit_plot_page():
    st.header("Expected Profit Plot (Weekly, Rolling EV)")

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

    # Load all relevant bets
    with with_cursor(bet_conn) as cur:
        cur.execute("""
            SELECT b.WagerID, b.PotentialPayout, b.DollarsAtStake, b.NetProfit,
                   b.DateTimePlaced, b.WLCA,
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

    # Compute weekly EV components
    weekly_dates = pd.date_range(start=start_date, end=end_date, freq="W-MON")
    results = []
    for week_end in weekly_dates:
        active = df[(df["WLCA"] == "Active") & (df["DateTimePlaced"] <= week_end)]
        resolved = df[(df["WLCA"].isin(["Win", "Loss", "Cashout"])) & (df["DateTimePlaced"] <= week_end)]

        pot = active["PotentialPayout"].astype(float).sum()
        stake = active["DollarsAtStake"].astype(float).sum()
        net = resolved["NetProfit"].astype(float).sum()

        expected_profit = pot - stake + net
        results.append({"week": week_end, "expected_profit": expected_profit})

    plot_df = pd.DataFrame(results)

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(plot_df["week"], plot_df["expected_profit"], marker="o")
    ax.set_title(f"Expected Profit — {sel_type}: {sel_lbl}")
    ax.set_xlabel("Week")
    ax.set_ylabel("Expected Profit ($)")
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
    plt.xticks(rotation=45)
    st.pyplot(fig, use_container_width=True)

# ───────────────── SIDEBAR NAV ─────────────────
page = st.sidebar.radio("Choose Page", ["Expected Profit Plot"])
if page == "Expected Profit Plot":
    expected_profit_plot_page()
