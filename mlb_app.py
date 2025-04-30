import streamlit as st
import mysql.connector
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta
from collections import defaultdict

# ─────────────────── STREAMLIT CONFIG ───────────────────
st.set_page_config(page_title="NBA MVP Probabilities", layout="wide")

# ─────────────────── MYSQL CONNECTION ───────────────────
def get_connection():
    return mysql.connector.connect(
        host=st.secrets["FUTURES_DB"]["host"],
        user=st.secrets["FUTURES_DB"]["user"],
        password=st.secrets["FUTURES_DB"]["password"],
        database=st.secrets["FUTURES_DB"]["database"]
    )

# ─────────────────── ODDS UTILS ───────────────────
def american_odds_to_probability(odds: int) -> float:
    if odds > 0:
        return 100.0 / (odds + 100.0)
    else:
        return float(abs(odds)) / (abs(odds) + 100.0)

# ─────────────────── MAIN PLOT FUNCTION ───────────────────
def plot_mvp_odds():
    st.subheader("NBA MVP Implied Probability Over Time")

    connection_futures = get_connection()
    cursor = connection_futures.cursor(dictionary=True)
    cursor.execute("SELECT DISTINCT team_name FROM NBAMVP;")
    rows = cursor.fetchall()
    cursor.close()

    all_participants = [r["team_name"] for r in rows]
    if not all_participants:
        st.warning("No participants found in NBAMVP.")
        connection_futures.close()
        return

    def get_latest_odds_as_of_date(participant: str, snapshot_datetime: datetime) -> int:
        cursor_odds = connection_futures.cursor(dictionary=True)
        query = """
        SELECT FanDuel FROM NBAMVP
        WHERE team_name = %s AND date_created <= %s
        ORDER BY date_created DESC LIMIT 1
        """
        cursor_odds.execute(query, (participant, snapshot_datetime))
        row = cursor_odds.fetchone()
        cursor_odds.close()
        if row and row["FanDuel"] is not None:
            try:
                return int(row["FanDuel"])
            except:
                return 0
        return 0

    start_date = datetime(2025, 1, 1)
    end_date   = datetime(2025, 4, 1)

    participant_probs = {p: [] for p in all_participants}
    current_date = start_date
    while current_date <= end_date:
        snapshot_dt = current_date.replace(hour=23, minute=59, second=59)
        for participant in all_participants:
            american_odds = get_latest_odds_as_of_date(participant, snapshot_dt)
            prob = american_odds_to_probability(american_odds) if american_odds != 0 else 0.0
            participant_probs[participant].append((current_date, prob))
        current_date += timedelta(days=1)

    connection_futures.close()

    # ─────────── Plotting ───────────
    fig, ax = plt.subplots(figsize=(11, 6))
    ax.set_facecolor("white")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.set_ylim(0, 1)
    ax.set_yticks([0, 0.2, 0.4, 0.6, 0.8, 1.0])
    ax.set_yticklabels(["0%", "20%", "40%", "60%", "80%", "100%"])
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b'))

    for participant, daily_data in participant_probs.items():
        daily_data_sorted = sorted(daily_data, key=lambda x: x[0])
        dates = [d[0] for d in daily_data_sorted]
        probs = [d[1] for d in daily_data_sorted]
        ax.plot(dates, probs, linewidth=2, label=participant)

    ax.legend(loc="upper right", frameon=False)
    ax.set_xlabel("")
    ax.set_ylabel("Implied Probability")
    plt.title("NBA MVP Implied Probability Over Time")
    plt.tight_layout()
    st.pyplot(fig)

# ─────────────────── STREAMLIT NAV ───────────────────
page = st.sidebar.radio("Choose Page", ["MVP Odds Plot"])

if page == "MVP Odds Plot":
    plot_mvp_odds()
