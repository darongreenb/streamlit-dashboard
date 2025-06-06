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
        "NBADefensivePotY", "NBAMIP", "NBARotY", "NBASixthMotY"
    ]
    division_tables = ["NBAAtlantic", "NBAPacific", "NBACentral", "NBASoutheast", "NBASouthwest", "NBANorthwest"]

    market_table = st.selectbox("Select Market Table", market_options + division_tables)
    col1, col2 = st.columns(2)
    start_date = col1.date_input("Start Date", datetime(2024, 12, 23))
    end_date = col2.date_input("End Date", datetime.today().date())

    top_k = st.slider("Number of Top Participants to Show", min_value=1, max_value=10, value=5)
    manual_selection_enabled = st.checkbox("Manually select participants")

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
        g = group.set_index("date")["prob"].reindex(date_range).ffill()
        g = g.reset_index().rename(columns={"index": "date"})
        g["team_name"] = name
        all_frames.append(g)
    daily = pd.concat(all_frames)

    if manual_selection_enabled:
        participants = sorted(daily["team_name"].unique().tolist())
        selected_participants = st.multiselect("Choose Participants to Display", participants)
        if not selected_participants:
            st.warning("Please select at least one participant.")
            return
        display_set = selected_participants
    else:
        last_day = daily[daily['date'] == daily['date'].max()]
        display_set = last_day.sort_values("prob", ascending=False).head(top_k)["team_name"].tolist()

    # ... [unchanged code above this point] ...

    daily_top = daily[daily["team_name"].isin(display_set)]

    fig, ax = plt.subplots(figsize=(12, 6))
    for name, grp in daily_top.groupby("team_name"):
        ax.plot(grp["date"], grp["prob"] * 100, label=name, linewidth=2)

    max_prob = daily_top["prob"].max()
    y_max = min(max_prob + 0.05, 1.0) * 100  # 5% headroom, capped at 100%

    ax.set_ylim(0, y_max)
    ax.set_ylabel("Implied Probability (%)")
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
    ax.yaxis.set_major_formatter(PercentFormatter())
    title_suffix = ", Selected Participants" if manual_selection_enabled else f" – Top {top_k}"
    ax.set_title(f"{market_table}{title_suffix} Implied Probabilities Over Time")
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    plt.xticks(rotation=45)
    ax.legend(title="Team Name", loc='best', frameon=False)
    plt.tight_layout()
    st.pyplot(fig)


# ────────────────────── RUN ──────────────────────
if __name__ == "__main__":
    main()
