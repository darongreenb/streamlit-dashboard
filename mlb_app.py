import streamlit as st
import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import PercentFormatter
from datetime import datetime, timedelta

# ────────────────────── CONFIG ──────────────────────
st.set_page_config(page_title="NBA Futures Probabilities", layout="wide")

# ────────────────────── DB CREDS ──────────────────────
FUTURES_DB = {
    "host": "greenalephfutures.cnwukek8ge3b.us-east-2.rds.amazonaws.com",
    "user": "admin",
    "password": "greenalephadmin",
    "database": "futuresdata"
}

# ────────────────────── UTILS ──────────────────────
def american_odds_to_probability(odds: int) -> float:
    if odds > 0:
        return 100.0 / (odds + 100.0)
    elif odds < 0:
        return abs(odds) / (abs(odds) + 100.0)
    return 0.0

# ────────────────────── MAIN STREAMLIT APP ──────────────────────
def main():
    st.title("NBA Futures – Implied Probability Tracker")

    market_options = [
        "NBAMVP", "NBAChampionship", "NBAEasternConference", "NBAWesternConference",
        "NBAAtlantic", "NBAPacific", "NBACentral", "NBASoutheast", "NBASouthwest",
        "NBANorthwest", "NBADefensivePotY", "NBAMIP", "NBARotY", "NBASixthMotY"
    ]

    market_table = st.selectbox("Select Market Table", market_options)
    col1, col2 = st.columns(2)
    start_date = col1.date_input("Start Date", datetime(2024, 12, 31))
    end_date   = col2.date_input("End Date", datetime(2025, 4, 30))

    if start_date > end_date:
        st.error("Start date must be before end date.")
        return

    if not st.button("Generate Plot"):
        return

    # DB Connection
    conn = mysql.connector.connect(**FUTURES_DB)
    query = f"""
    SELECT team_name, date_created,
           BetMGM, DraftKings, Caesars, ESPNBet, FanDuel, BallyBet, RiversCasino, Bet365
    FROM {market_table}
    WHERE date_created BETWEEN %s AND %s
    ORDER BY team_name, date_created
    """
    df = pd.read_sql(query, conn, params=(f"{start_date} 00:00:00", f"{end_date} 23:59:59"))
    conn.close()

    if df.empty:
        st.warning("No odds data returned for the selected market.")
        return

    df['date'] = pd.to_datetime(df['date_created']).dt.date
    odds_cols = ["BetMGM","DraftKings","Caesars","ESPNBet","FanDuel","BallyBet","RiversCasino","Bet365"]
    df[odds_cols] = df[odds_cols].apply(pd.to_numeric, errors='coerce').fillna(0)
    df['best'] = df[odds_cols].replace(0, pd.NA).max(axis=1).fillna(0).astype(int)
    df['prob'] = df['best'].apply(american_odds_to_probability)

    latest = df.sort_values(['team_name', 'date']).groupby(['team_name','date']).tail(1)
    date_range = pd.date_range(start_date, end_date, freq='D')

    all_frames = []
    for name, group in latest.groupby("team_name"):
        g = group.set_index("date")[["prob"]].reindex(date_range).ffill()
        g = g.reset_index().rename(columns={"index": "date"})
        g["team_name"] = name
        all_frames.append(g)
    daily = pd.concat(all_frames)

    last_day = daily[daily['date'] == daily['date'].max()]
    top5 = last_day.sort_values("prob", ascending=False).head(5)["team_name"].tolist()
    daily_top = daily[daily["team_name"].isin(top5)]

    fig, ax = plt.subplots(figsize=(12, 6))
    for name, grp in daily_top.groupby("team_name"):
        ax.plot(grp["date"], grp["prob"] * 100, label=name, linewidth=2)

    ax.set_ylim(0, 100)
    ax.set_ylabel("Implied Probability (%)")
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
    ax.yaxis.set_major_formatter(PercentFormatter())
    ax.set_title(f"{market_table} – Top 5 Implied Probabilities Over Time")
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    plt.xticks(rotation=45)
    ax.legend(title="Name", bbox_to_anchor=(1.02, 1), loc='upper left')
    plt.tight_layout()
    st.pyplot(fig)

# ────────────────────── RUN ──────────────────────
if __name__ == "__main__":
    main()
