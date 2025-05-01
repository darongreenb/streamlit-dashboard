import streamlit as st
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import PercentFormatter
import mysql.connector
import pymysql, re
from collections import defaultdict

# ────────────────────── CONFIG ──────────────────────
st.set_page_config(page_title="NBA Futures Dashboard", layout="wide")
st.sidebar.title("📊 Dashboard Navigation")

# ────────────────────── DB CREDS ──────────────────────
FUTURES_DB = {
    "host": "greenalephfutures.cnwukek8ge3b.us-east-2.rds.amazonaws.com",
    "user": "admin",
    "password": "greenalephadmin",
    "database": "futuresdata"
}

BETTING_DB = {
    "host": "betting-db.cp86ssaw6cm7.us-east-1.rds.amazonaws.com",
    "user": "admin",
    "password": "7nRB1i2&A-K>",
    "database": "betting_db"
}

# ────────────────────── UTILS ──────────────────────
def american_odds_to_probability(odds: int) -> float:
    if odds > 0:
        return 100.0 / (odds + 100.0)
    elif odds < 0:
        return abs(odds) / (abs(odds) + 100.0)
    return 0.0

def american_odds_to_decimal(o): return 1.0 + (o/100) if o > 0 else 1.0 + 100/abs(o) if o else 1.0

def american_odds_to_prob(o): return 100/(o+100) if o > 0 else abs(o)/(abs(o)+100) if o else 0.0

def cast_odds(v):
    if v in (None, "", 0): return 0
    if isinstance(v, (int, float)): return int(v)
    m = re.search(r"[-+]?\d+", str(v))
    return int(m.group()) if m else 0

def new_betting_conn():
    return pymysql.connect(**BETTING_DB, cursorclass=pymysql.cursors.DictCursor, autocommit=True)

def new_futures_conn():
    return pymysql.connect(**FUTURES_DB, cursorclass=pymysql.cursors.DictCursor, autocommit=True)

def with_cursor(conn):
    conn.ping(reconnect=True)
    return conn.cursor()

# ────────────────────── FUTURES MAPS ──────────────────────
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
        cur.execute(
            f"""SELECT {','.join(sportsbook_cols)}
                  FROM {tbl}
                 WHERE team_name = %s AND date_created <= %s
              ORDER BY date_created DESC LIMIT 1""",
            (alias, cutoff_dt)
        )
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

# ────────────────────── MAIN ──────────────────────
page = st.sidebar.radio("Choose a Page", ["Implied Probability Tracker", "EV Table"])

if page == "Implied Probability Tracker":
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
    else:
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
            else:
                display_set = selected_participants
        else:
            last_day = daily[daily['date'] == daily['date'].max()]
            display_set = last_day.sort_values("prob", ascending=False).head(top_k)["team_name"].tolist()

        if 'display_set' in locals():
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

elif page == "EV Table":
    st.markdown("<h1 style='text-align: center;'>NBA Futures EV Table</h1>", unsafe_allow_html=True)
    st.markdown("<h3 style='text-align: center; color: gray;'>among markets tracked in <code>futures_db</code></h3>", unsafe_allow_html=True)

    bet_conn = new_betting_conn()
    fut_conn = new_futures_conn()
    now = datetime.utcnow()

    st.markdown("### 🧹 Customize Vig by Market")
    vig_inputs = {}
    unique_markets = sorted(set((et, el) for et, el in futures_table_map))
    with st.expander("Set Vig Percentage Per Market", expanded=False):
        for et, el in unique_markets:
            key = f"{et}|{el}"
            percent = st.slider(
                label=f"{et} — {el}", min_value=0, max_value=20,
                value=5, step=1, key=key
            )
            vig_inputs[(et, el)] = percent / 100.0

    sql_active = """
        SELECT b.WagerID, b.PotentialPayout, b.DollarsAtStake,
               l.EventType, l.EventLabel, l.ParticipantName
          FROM bets b JOIN legs l ON b.WagerID = l.WagerID
         WHERE b.WhichBankroll='GreenAleph' AND b.WLCA='Active' AND l.LeagueName='NBA'
    """
    with with_cursor(bet_conn) as cur:
        cur.execute(sql_active)
        rows = cur.fetchall()

    active_bets = defaultdict(lambda: {"pot":0,"stake":0,"legs":[]})
    for r in rows:
        w = active_bets[r["WagerID"]]
        w["pot"]   = w["pot"]   or float(r["PotentialPayout"] or 0)
        w["stake"] = w["stake"] or float(r["DollarsAtStake"] or 0)
        w["legs"].append((r["EventType"], r["EventLabel"], r["ParticipantName"]))

    active_stake, active_exp = defaultdict(float), defaultdict(float)
    for data in active_bets.values():
        pot, stake, legs = data["pot"], data["stake"], data["legs"]
        decs = []; prob = 1.0
        for et,el,pn in legs:
            dec,p = best_odds_decimal_prob(et,el,pn,now,fut_conn,vig_inputs)
            if p == 0: prob = 0; break
            decs.append((dec,et,el)); prob *= p
        if prob == 0: continue
        expected = pot * prob
        sum_exc  = sum(d-1 for d,_,_ in decs)
        if sum_exc <= 0: continue
        for d,et,el in decs:
            w = (d-1)/sum_exc
            active_stake[(et,el)] += w*stake
            active_exp  [(et,el)] += w*expected

    sql_real = """
        SELECT b.WagerID, b.NetProfit,
               l.EventType, l.EventLabel, l.ParticipantName
          FROM bets b JOIN legs l ON b.WagerID = l.WagerID
         WHERE b.WhichBankroll='GreenAleph'
           AND b.WLCA IN ('Win','Loss','Cashout')
           AND l.LeagueName='NBA'
    """
    with with_cursor(bet_conn) as cur:
        cur.execute(sql_real)
        rows = cur.fetchall()

    wager_net  = defaultdict(float)
    wager_legs = defaultdict(list)
    for r in rows:
        wager_net [r["WagerID"]] = float(r["NetProfit"] or 0)
        wager_legs[r["WagerID"]].append((r["EventType"],r["EventLabel"],r["ParticipantName"]))

    realized_np = defaultdict(float)
    for wid,legs in wager_legs.items():
        net  = wager_net[wid]
        decs = [(best_odds_decimal_prob(et,el,pn,now,fut_conn,vig_inputs)[0], et, el) for et,el,pn in legs]
        sum_exc = sum(d-1 for d,_,_ in decs)
        if sum_exc <= 0: continue
        for d,et,el in decs:
            realized_np[(et,el)] += net * ((d-1)/sum_exc)

    bet_conn.close(); fut_conn.close()

    keys = set(active_stake)|set(active_exp)|set(realized_np)
    out  = []
    for et,el in sorted(keys):
        stake = active_stake.get((et,el),0)
        exp   = active_exp.get((et,el),0)
        net   = realized_np.get((et,el),0)
        out.append(dict(EventType=et, EventLabel=el,
                        ActiveDollarsAtStake = round(stake,2),
                        ActiveExpectedPayout = round(exp  ,2),
                        RealizedNetProfit    = round(net  ,2),
                        ExpectedValue        = round(exp-stake+net,2)))
    df = pd.DataFrame(out).sort_values(["EventType","EventLabel"]).reset_index(drop=True)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("💸 Active Stake", f"${df['ActiveDollarsAtStake'].sum():,.0f}")
    col2.metric("📈 Expected Payout", f"${df['ActiveExpectedPayout'].sum():,.0f}")
    col3.metric("💰 Realized Net Profit", f"${df['RealizedNetProfit'].sum():,.0f}")
    col4.metric("⚡️ Expected Value", f"${df['ExpectedValue'].sum():,.0f}")

    def highlight_ev(val):
        color = "green" if val > 0 else "red" if val < 0 else "black"
        return f"color: {color}; font-weight: bold"

    styled_df = df.style.format("${:,.0f}", subset=[
        "ActiveDollarsAtStake", "ActiveExpectedPayout", "RealizedNetProfit", "ExpectedValue"]) \
        .applymap(highlight_ev, subset=["ExpectedValue"])

    st.markdown("### Market-Level Breakdown")
    st.dataframe(styled_df, use_container_width=True, height=700)
