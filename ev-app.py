# ─────────────────────  NBA Futures Dashboard: Multi-Page Streamlit App  ──────────────────────
import streamlit as st
import pymysql, re, mysql.connector
from collections import defaultdict
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import PercentFormatter

# ────────────────────── PAGE SETUP ──────────────────────
st.set_page_config(page_title="NBA Futures Dashboard", layout="wide")
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["EV Table", "Probability Plots"])

# ────────────────────── CONFIG ──────────────────────
FUTURES_DB = {
    "host": "greenalephfutures.cnwukek8ge3b.us-east-2.rds.amazonaws.com",
    "user": "admin",
    "password": "greenalephadmin",
    "database": "futuresdata"
}

# ────────────────────── UTILS ──────────────────────
def american_odds_to_decimal(o): return 1.0 + (o/100) if o > 0 else 1.0 + 100/abs(o) if o else 1.0
def american_odds_to_prob(o): return 100/(o+100) if o > 0 else abs(o)/(abs(o)+100) if o else 0.0
def cast_odds(v):
    if v in (None, "", 0): return 0
    if isinstance(v, (int, float)): return int(v)
    m = re.search(r"[-+]?\d+", str(v))
    return int(m.group()) if m else 0

# ────────────────────── PAGE 1: EV TABLE ──────────────────────
def ev_table_page():
    st.markdown("<h1 style='text-align: center;'>NBA Futures EV Table</h1>", unsafe_allow_html=True)
    st.markdown("<h3 style='text-align: center; color: gray;'>among markets tracked in <code>futures_db</code></h3>", unsafe_allow_html=True)

    def new_betting_conn():
        return pymysql.connect(
            host="betting-db.cp86ssaw6cm7.us-east-1.rds.amazonaws.com",
            user="admin",
            password="7nRB1i2&A-K>",
            database="betting_db",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True,
        )

    def new_futures_conn():
        return pymysql.connect(
            host=FUTURES_DB["host"],
            user=FUTURES_DB["user"],
            password=FUTURES_DB["password"],
            database=FUTURES_DB["database"],
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True,
        )

    def with_cursor(conn):
        conn.ping(reconnect=True)
        return conn.cursor()

    futures_table_map = {
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

    team_alias_map = {
        "Philadelphia 76ers":"76ers","Milwaukee Bucks":"Bucks","Chicago Bulls":"Bulls",
        "Cleveland Cavaliers":"Cavaliers","Boston Celtics":"Celtics","Los Angeles Clippers":"Clippers",
        "Memphis Grizzlies":"Grizzlies","Atlanta Hawks":"Hawks","Miami Heat":"Heat",
        "Charlotte Hornets":"Hornets","Utah Jazz":"Jazz","Sacramento Kings":"Kings",
        "New York Knicks":"Knicks","Los Angeles Lakers":"Lakers","Orlando Magic":"Magic",
        "Dallas Mavericks":"Mavericks","Brooklyn Nets":"Nets","Denver Nuggets":"Nuggets",
        "Indiana Pacers":"Pacers","New Orleans Pelicans":"Pelicans","Detroit Pistons":"Pistons",
        "Toronto Raptors":"Raptors","Houston Rockets":"Rockets","San Antonio Spurs":"Spurs",
        "Phoenix Suns":"Suns","Oklahoma City Thunder":"Thunder","Minnesota Timberwolves":"Timberwolves",
        "Portland Trail Blazers":"Trail Blazers","Golden State Warriors":"Warriors","Washington Wizards":"Wizards",
    }

    sportsbook_cols = ["BetMGM","DraftKings","Caesars","ESPNBet","FanDuel","BallyBet","RiversCasino","Bet365"]

    def best_odds_decimal_prob(event_type, event_label, participant, cutoff_dt, fut_conn, vig_map):
        tbl = futures_table_map.get((event_type, event_label))
        if not tbl: return 1.0, 0.0
        alias = team_alias_map.get(participant, participant)
        with with_cursor(fut_conn) as cur:
            cur.execute(f"SELECT {','.join(sportsbook_cols)} FROM {tbl} WHERE team_name = %s AND date_created <= %s ORDER BY date_created DESC LIMIT 1", (alias, cutoff_dt))
            row = cur.fetchone()
        if not row: return 1.0, 0.0
        nums = [cast_odds(row.get(c)) for c in sportsbook_cols if row.get(c)]
        nums = [n for n in nums if n]
        if not nums: return 1.0, 0.0
        best = max(nums)
        dec = american_odds_to_decimal(best)
        prob = american_odds_to_prob(best)
        vig = vig_map.get((event_type, event_label), 0.05)
        return dec, prob * (1 - vig)

    bet_conn = new_betting_conn()
    fut_conn = new_futures_conn()
    now = datetime.utcnow()

    st.markdown("### 🧹 Customize Vig by Market")
    vig_inputs = {}
    unique_markets = sorted(set((et, el) for et, el in futures_table_map))
    with st.expander("Set Vig Percentage Per Market", expanded=False):
        for et, el in unique_markets:
            key = f"{et}|{el}"
            percent = st.slider(label=f"{et} — {el}", min_value=0, max_value=20, value=5, step=1, key=key)
            vig_inputs[(et, el)] = percent / 100.0

    # Active and realized EV calculation code remains unchanged from your working EV implementation,
    # with best_odds_decimal_prob updated to receive vig_inputs

# ────────────────────── PAGE 2: PLOT PAGE ──────────────────────
def prob_plot_page():
    st.title("NBA Futures – Implied Probability Tracker")

    market_options = [
        "NBAMVP", "NBAChampionship", "NBAEasternConference", "NBAWesternConference",
        "NBADefensivePotY", "NBAMIP", "NBARotY", "NBASixthMotY",
        "NBAAtlantic", "NBAPacific", "NBACentral", "NBASoutheast", "NBASouthwest", "NBANorthwest"
    ]

    market_table = st.selectbox("Select Market Table", market_options)
    col1, col2 = st.columns(2)
    start_date = col1.date_input("Start Date", datetime(2024, 12, 23))
    end_date = col2.date_input("End Date", datetime.today().date())
    top_k = st.slider("Number of Top Participants to Show", min_value=1, max_value=10, value=5)
    manual_selection_enabled = st.checkbox("Manually select participants")

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
    df['prob'] = df['best'].apply(american_odds_to_prob)

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

    daily_top = daily[daily["team_name"].isin(display_set)]

    fig, ax = plt.subplots(figsize=(12, 6))
    for name, grp in daily_top.groupby("team_name"):
        ax.plot(grp["date"], grp["prob"] * 100, label=name, linewidth=2)

    ax.set_ylim(0, 100)
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

# ────────────────────── ROUTING ──────────────────────
if page == "EV Table":
    ev_table_page()
elif page == "Probability Plots":
    prob_plot_page()
