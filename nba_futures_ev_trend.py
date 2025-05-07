import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import glob
from datetime import datetime, timedelta
import pymysql
import json
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path

# ─────────────────  PAGE CONFIG  ─────────────────
st.set_page_config(page_title="NBA Futures EV Trend", layout="wide")
st.markdown("<h1 style='text-align: center;'>NBA Futures EV Trend</h1>", unsafe_allow_html=True)
st.markdown("<h3 style='text-align: center; color: gray;'>Historical Expected Value of NBA Futures Portfolio</h3>", unsafe_allow_html=True)

# ─────────────────  CONSTANTS  ─────────────────
# Directory to store EV snapshots
DATA_DIR = "ev_snapshots"

# Ensure data directory exists
Path(DATA_DIR).mkdir(exist_ok=True)

# ─────────────────  DB HELPERS  ──────────────────
def new_betting_conn():
    """Create a new connection to the betting database with improved error handling"""
    try:
        conn = pymysql.connect(
            host="betting-db.cp86ssaw6cm7.us-east-1.rds.amazonaws.com",
            user="admin",
            password="7nRB1i2&A-K>",
            database="betting_db",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True,
            connect_timeout=10  # Add timeout to prevent hanging
        )
        return conn
    except pymysql.Error as e:
        error_code = e.args[0]
        error_message = e.args[1] if len(e.args) > 1 else str(e)
        
        # Log specific error for debugging
        st.error(f"Failed to connect to betting database. Error: {error_code}")
        
        if error_code == 1045:  # Access denied error
            st.error("Database authentication failed. Please check your credentials.")
        elif error_code == 2003:  # Can't connect error
            st.error("Cannot connect to the database server. The server might be down or not accessible from Streamlit Cloud.")
            
        # Return None to allow graceful handling of connection failure
        return None

def new_futures_conn():
    """Create a new connection to the futures database with improved error handling"""
    try:
        conn = pymysql.connect(
            host="greenalephfutures.cnwukek8ge3b.us-east-2.rds.amazonaws.com",
            user="admin",
            password="greenalephadmin",
            database="futuresdata",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True,
            connect_timeout=10
        )
        return conn
    except pymysql.Error as e:
        error_code = e.args[0]
        error_message = e.args[1] if len(e.args) > 1 else str(e)
        
        st.error(f"Failed to connect to futures database. Error: {error_code}")
        
        if error_code == 1045:
            st.error("Database authentication failed. Please check your credentials.")
        elif error_code == 2003:
            st.error("Cannot connect to the database server. The server might be down or not accessible from Streamlit Cloud.")
            
        return None

def with_cursor(conn):
    """Create a cursor with error handling"""
    if conn is None:
        return None
    
    try:
        conn.ping(reconnect=True)
        return conn.cursor()
    except Exception as e:
        st.error(f"Error creating cursor: {str(e)}")
        return None

# ────────────────  EV CALCULATION  ────────────────
def calculate_total_ev(as_of_date=None):
    """
    Calculate the total Expected Value for the given date
    Uses code from the original EV Table app with modifications
    for historical calculation
    """
    import re
    from collections import defaultdict
    
    # Import needed functions and maps from the original code
    # Odds helpers
    def american_odds_to_decimal(o): return 1.0 + (o/100) if o > 0 else 1.0 + 100/abs(o) if o else 1.0
    def american_odds_to_prob(o): return 100/(o+100) if o > 0 else abs(o)/(abs(o)+100) if o else 0.0
    def cast_odds(v):
        if v in (None, "", 0): return 0
        if isinstance(v, (int, float)): return int(v)
        m = re.search(r"[-+]?\d+", str(v))
        return int(m.group()) if m else 0
    
    # Maps
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
        """Get best odds with error handling"""
        if fut_conn is None:
            return 1.0, 0.0
            
        tbl = futures_table_map.get((event_type, event_label))
        if not tbl: return 1.0, 0.0
        
        alias = team_alias_map.get(participant, participant)
        
        cursor = with_cursor(fut_conn)
        if cursor is None:
            return 1.0, 0.0
            
        try:
            cursor.execute(
                f"""SELECT {','.join(sportsbook_cols)}
                      FROM {tbl}
                     WHERE team_name = %s AND date_created <= %s
                  ORDER BY date_created DESC LIMIT 1""",
                (alias, cutoff_dt)
            )
            row = cursor.fetchone()
        except Exception as e:
            st.error(f"Error querying odds data: {str(e)}")
            return 1.0, 0.0
            
        if not row: return 1.0, 0.0
        nums = [cast_odds(row.get(c)) for c in sportsbook_cols if row.get(c)]
        nums = [n for n in nums if n]
        if not nums: return 1.0, 0.0
        best = max(nums)
        dec = american_odds_to_decimal(best)
        prob = american_odds_to_prob(best)
        vig = vig_map.get((event_type, event_label), 0.05)
        return dec, prob * (1 - vig)

    # Default to current time if no date specified
    cutoff_dt = as_of_date if as_of_date else datetime.utcnow()
    
    # Attempt to connect to databases
    bet_conn = new_betting_conn()
    fut_conn = new_futures_conn()
    
    # Check if either connection failed
    if bet_conn is None or fut_conn is None:
        st.warning("⚠️ Unable to connect to databases. Cannot calculate EV.")
        return None

    # Set default vig of 5% for all markets
    vig_inputs = defaultdict(lambda: 0.05)

    # ------- Active wagers -------
    sql_active = """
        SELECT b.WagerID, b.PotentialPayout, b.DollarsAtStake,
            l.EventType, l.EventLabel, l.ParticipantName, b.PlacedDateTime
        FROM bets b JOIN legs l ON b.WagerID = l.WagerID
        WHERE b.WhichBankroll='GreenAleph' AND b.WLCA='Active' AND l.LeagueName='NBA'
        AND b.PlacedDateTime <= %s
    """
    
    cursor = with_cursor(bet_conn)
    if cursor is None:
        st.error("Failed to create cursor for betting database")
        return None
        
    try:
        cursor.execute(sql_active, (cutoff_dt,))
        rows = cursor.fetchall()
    except Exception as e:
        st.error(f"Error querying active wagers: {str(e)}")
        rows = []

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
            dec,p = best_odds_decimal_prob(et,el,pn,cutoff_dt,fut_conn,vig_inputs)
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

    # ------- Realised net profit -------
    sql_real = """
        SELECT b.WagerID, b.NetProfit,
            l.EventType, l.EventLabel, l.ParticipantName, b.SettledDateTime
        FROM bets b JOIN legs l ON b.WagerID = l.WagerID
        WHERE b.WhichBankroll='GreenAleph'
        AND b.WLCA IN ('Win','Loss','Cashout')
        AND l.LeagueName='NBA'
        AND b.SettledDateTime <= %s
    """
    
    try:
        cursor.execute(sql_real, (cutoff_dt,))
        rows = cursor.fetchall()
    except Exception as e:
        st.error(f"Error querying realized profits: {str(e)}")
        rows = []

    wager_net  = defaultdict(float)
    wager_legs = defaultdict(list)
    for r in rows:
        wager_net [r["WagerID"]] = float(r["NetProfit"] or 0)
        wager_legs[r["WagerID"]].append((r["EventType"],r["EventLabel"],r["ParticipantName"]))

    realized_np = defaultdict(float)
    for wid,legs in wager_legs.items():
        net  = wager_net[wid]
        decs = [(best_odds_decimal_prob(et,el,pn,cutoff_dt,fut_conn,vig_inputs)[0], et, el) for et,el,pn in legs]
        sum_exc = sum(d-1 for d,_,_ in decs)
        if sum_exc <= 0: continue
        for d,et,el in decs:
            realized_np[(et,el)] += net * ((d-1)/sum_exc)

    # Close database connections
    if bet_conn:
        bet_conn.close()
    if fut_conn:
        fut_conn.close()

    # ------- Calculate total EV -------
    total_ev = 0
    keys = set(active_stake)|set(active_exp)|set(realized_np)
    
    for et,el in keys:
        stake = active_stake.get((et,el),0)
        exp   = active_exp.get((et,el),0)
        net   = realized_np.get((et,el),0)
        total_ev += (exp - stake + net)
    
    return total_ev

# ────────────────  SNAPSHOT FUNCTIONS  ────────────────
def save_ev_snapshot(date, ev_value):
    """Save a snapshot of the EV value for a specific date"""
    filename = f"{DATA_DIR}/ev_{date.strftime('%Y-%m-%d')}.json"
    data = {
        "date": date.strftime("%Y-%m-%d"),
        "ev": ev_value,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    with open(filename, 'w') as f:
        json.dump(data, f)
    
    return filename

def get_all_snapshots():
    """Get all stored EV snapshots and return as a dataframe"""
    files = sorted(glob.glob(f"{DATA_DIR}/ev_*.json"))
    data = []
    
    for file in files:
        try:
            with open(file, 'r') as f:
                snapshot = json.load(f)
                data.append({
                    "date": datetime.strptime(snapshot["date"], "%Y-%m-%d"),
                    "ev": snapshot["ev"]
                })
        except Exception as e:
            st.warning(f"Failed to read snapshot file {file}: {str(e)}")
    
    if data:
        return pd.DataFrame(data)
    else:
        return pd.DataFrame(columns=["date", "ev"])

def generate_new_snapshot():
    """Calculate and save a new EV snapshot for today"""
    today = datetime.now().date()
    today_dt = datetime.combine(today, datetime.min.time())
    
    with st.spinner("Calculating current EV..."):
        ev = calculate_total_ev(today_dt)
    
    if ev is not None:
        filename = save_ev_snapshot(today_dt, ev)
        st.success(f"Created new EV snapshot for {today}: ${ev:,.2f}")
        return True
    else:
        st.error("Failed to calculate EV value")
        return False

def generate_historical_snapshot(target_date):
    """Calculate and save a historical EV snapshot for the specified date"""
    target_dt = datetime.combine(target_date, datetime.min.time())
    
    with st.spinner(f"Calculating historical EV for {target_date}..."):
        ev = calculate_total_ev(target_dt)
    
    if ev is not None:
        filename = save_ev_snapshot(target_dt, ev)
        st.success(f"Created historical EV snapshot for {target_date}: ${ev:,.2f}")
        return True
    else:
        st.error(f"Failed to calculate EV value for {target_date}")
        return False

# ────────────────  VISUALIZATION FUNCTIONS  ────────────────
def plot_ev_trend(df):
    """Create an interactive plot of the EV trend"""
    if df.empty:
        st.warning("No EV data available to plot")
        return
    
    # Create the Plotly figure
    fig = px.line(
        df, 
        x="date", 
        y="ev",
        title="NBA Futures Portfolio Expected Value Over Time",
        labels={"date": "Date", "ev": "Expected Value ($)"},
        markers=True
    )
    
    # Add a zero line for reference
    fig.add_hline(y=0, line_dash="dash", line_color="red")
    
    # Customize the layout
    fig.update_layout(
        xaxis_title="Date",
        yaxis_title="Expected Value ($)",
        hovermode="x unified",
        legend_title="Legend",
        template="plotly_white",
        height=600
    )
    
    # Format y-axis as currency
    fig.update_yaxes(tickprefix="$", tickformat=",")
    
    # Add a trendline
    if len(df) > 1:
        x_numeric = np.arange(len(df))
        z = np.polyfit(x_numeric, df['ev'], 1)
        p = np.poly1d(z)
        trend_line = p(x_numeric)
        
        fig.add_trace(
            go.Scatter(
                x=df['date'],
                y=trend_line,
                mode='lines',
                name='Trend',
                line=dict(color='rgba(0, 100, 80, 0.7)', dash='dot')
            )
        )
    
    # Add range slider for time selection
    fig.update_layout(
        xaxis=dict(
            rangeslider=dict(visible=True),
            type="date"
        )
    )
    
    # Show the plot
    st.plotly_chart(fig, use_container_width=True)
    
    # Calculate and display statistics
    st.subheader("EV Statistics")
    
    # Create metrics in columns
    col1, col2, col3, col4 = st.columns(4)
    
    col1.metric("Current EV", f"${df['ev'].iloc[-1]:,.2f}")
    
    if len(df) > 1:
        # Calculate change from start
        change_from_start = df['ev'].iloc[-1] - df['ev'].iloc[0]
        col2.metric("Change from Start", 
                   f"${change_from_start:,.2f}", 
                   f"{(change_from_start / df['ev'].iloc[0] * 100) if df['ev'].iloc[0] != 0 else 0:.1f}%")
        
        # Weekly change
        if len(df) > 1:
            weekly_change = df['ev'].iloc[-1] - df['ev'].iloc[-2]
            col3.metric("Weekly Change", 
                       f"${weekly_change:,.2f}", 
                       f"{(weekly_change / df['ev'].iloc[-2] * 100) if df['ev'].iloc[-2] != 0 else 0:.1f}%")
        
        # Average weekly growth
        if len(df) > 1:
            weeks = (df['date'].iloc[-1] - df['date'].iloc[0]).days / 7
            avg_weekly_growth = (df['ev'].iloc[-1] - df['ev'].iloc[0]) / weeks if weeks > 0 else 0
            col4.metric("Avg Weekly Growth", f"${avg_weekly_growth:,.2f}")

# ────────────────  MAIN APP  ────────────────
def main():
    # Load existing snapshot data
    df_snapshots = get_all_snapshots()
    
    # App tabs
    tab1, tab2 = st.tabs(["EV Trend Visualization", "Manage Snapshots"])
    
    # Tab 1: Visualization
    with tab1:
        if df_snapshots.empty:
            st.warning("No EV snapshots found. Use the 'Manage Snapshots' tab to create snapshots.")
        else:
            # Sort by date to ensure chronological order
            df_snapshots = df_snapshots.sort_values("date")
            
            # Plot the trend
            plot_ev_trend(df_snapshots)
    
    # Tab 2: Manage Snapshots
    with tab2:
        st.subheader("Create New Snapshot")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("Create Snapshot for Today"):
                generate_new_snapshot()
        
        with col2:
            historical_date = st.date_input(
                "Select date for historical snapshot",
                max_value=datetime.now().date()
            )
            if st.button("Create Historical Snapshot"):
                generate_historical_snapshot(historical_date)
        
        # Display existing snapshots
        st.subheader("Existing Snapshots")
        
        if df_snapshots.empty:
            st.info("No snapshots found")
        else:
            # Add formatted EV column for display
            display_df = df_snapshots.copy()
            display_df["formatted_ev"] = display_df["ev"].apply(lambda x: f"${x:,.2f}")
            
            # Display as a table
            st.dataframe(display_df[["date", "formatted_ev"]].rename(
                columns={"date": "Date", "formatted_ev": "Expected Value"}),
                use_container_width=True
            )
            
            # Option to delete snapshots
            if st.button("Delete All Snapshots"):
                if st.checkbox("I confirm I want to delete all snapshots"):
                    try:
                        for file in glob.glob(f"{DATA_DIR}/ev_*.json"):
                            os.remove(file)
                        st.success("All snapshots deleted")
                        st.experimental_rerun()
                    except Exception as e:
                        st.error(f"Error deleting snapshots: {str(e)}")

if __name__ == "__main__":
    main()
