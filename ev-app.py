# ─────────────────────  NBA Futures Dashboard: Historical EV Page  ──────────────────────
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import pymysql
from collections import defaultdict
import re

# ─────────────────  PAGE CONFIG  ─────────────────
st.set_page_config(page_title="NBA Futures Historical EV", layout="wide")
st.markdown("<h1 style='text-align: center;'>NBA Futures Historical EV</h1>", unsafe_allow_html=True)
st.markdown("<h3 style='text-align: center; color: gray;'>Weekly EV Tracking</h3>", unsafe_allow_html=True)

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
            st.info("If you're running on Streamlit Cloud, make sure your database is accessible from external networks.")
        
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
            connect_timeout=10  # Add timeout to prevent hanging
        )
        return conn
    except pymysql.Error as e:
        error_code = e.args[0]
        error_message = e.args[1] if len(e.args) > 1 else str(e)
        
        # Log specific error for debugging
        st.error(f"Failed to connect to futures database. Error: {error_code}")
        
        if error_code == 1045:  # Access denied error
            st.error("Database authentication failed. Please check your credentials.")
        elif error_code == 2003:  # Can't connect error
            st.error("Cannot connect to the database server. The server might be down or not accessible from Streamlit Cloud.")
            st.info("If you're running on Streamlit Cloud, make sure your database is accessible from external networks.")
        
        # Return None to allow graceful handling of connection failure
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

# ─────────────────  ODDS HELPERS  ────────────────
def american_odds_to_decimal(o): return 1.0 + (o/100) if o > 0 else 1.0 + 100/abs(o) if o else 1.0
def american_odds_to_prob(o): return 100/(o+100) if o > 0 else abs(o)/(abs(o)+100) if o else 0.0
def cast_odds(v):
    if v in (None, "", 0): return 0
    if isinstance(v, (int, float)): return int(v)
    m = re.search(r"[-+]?\d+", str(v))
    return int(m.group()) if m else 0

# ─────────────────  MAPS  ────────────────────────
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

# ─────────────────  HISTORICAL CALCULATION HELPERS  ────────────────
def get_weekly_dates(start_date, end_date):
    """Generate weekly dates from start to end date"""
    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current)
        current += timedelta(days=7)
    return dates

def get_ev_for_date(snapshot_date, bet_conn, fut_conn, vig_map):
    """Calculate EV for a specific date"""
    if bet_conn is None or fut_conn is None:
        return 0.0, 0.0, 0.0, 0.0
        
    # ------- Active wagers as of snapshot_date -------
    sql_active = """
        SELECT b.WagerID, b.PotentialPayout, b.DollarsAtStake,
            l.EventType, l.EventLabel, l.ParticipantName
        FROM bets b JOIN legs l ON b.WagerID = l.WagerID
        WHERE b.WhichBankroll='GreenAleph' 
        AND b.DateTimePlaced <= %s
        AND (b.WLCA='Active' OR (b.WLCA IN ('Win', 'Loss', 'Cashout') AND b.DateTimePlaced <= %s))
        AND l.LeagueName='NBA'
    """
    
    cursor = with_cursor(bet_conn)
    if cursor is None:
        return 0.0, 0.0, 0.0, 0.0
        
    try:
        cursor.execute(sql_active, (snapshot_date, snapshot_date))
        rows = cursor.fetchall()
    except Exception as e:
        st.error(f"Error querying active wagers for {snapshot_date}: {str(e)}")
        return 0.0, 0.0, 0.0, 0.0

    active_bets = defaultdict(lambda: {"pot":0, "stake":0, "legs":[], "status":""})
    
    # Get statuses for all bets
    sql_status = """
        SELECT WagerID, WLCA
        FROM bets
        WHERE WhichBankroll='GreenAleph'
        AND DateTimePlaced <= %s
    """
    try:
        cursor.execute(sql_status, (snapshot_date,))
        statuses = {row["WagerID"]: row["WLCA"] for row in cursor.fetchall()}
    except Exception as e:
        st.error(f"Error querying bet statuses for {snapshot_date}: {str(e)}")
        statuses = {}

    for r in rows:
        wager_id = r["WagerID"]
        status = statuses.get(wager_id, "Unknown")
        
        w = active_bets[wager_id]
        w["pot"] = w["pot"] or float(r["PotentialPayout"] or 0)
        w["stake"] = w["stake"] or float(r["DollarsAtStake"] or 0)
        w["status"] = status
        w["legs"].append((r["EventType"], r["EventLabel"], r["ParticipantName"]))

    # Calculate EV components
    total_active_stake = 0.0
    total_expected_payout = 0.0
    total_realized_profit = 0.0
    
    for wager_id, data in active_bets.items():
        pot = data["pot"]
        stake = data["stake"]
        legs = data["legs"]
        status = data["status"]

        # For settled bets
        if status in ['Win', 'Loss', 'Cashout']:
            if status == 'Win':
                total_realized_profit += (pot - stake)
            elif status == 'Loss':
                total_realized_profit -= stake
            elif status == 'Cashout':
                # For cashouts, we'd need the actual cashout amount
                # This is simplified and assumes breakeven
                pass
            continue
            
        # For active bets
        decs = []
        prob = 1.0
        for et, el, pn in legs:
            dec, p = best_odds_decimal_prob(et, el, pn, snapshot_date, fut_conn, vig_map)
            if p == 0:
                prob = 0
                break
            decs.append(dec)
            prob *= p
            
        if prob == 0:
            continue
            
        total_active_stake += stake
        total_expected_payout += pot * prob

    total_ev = total_expected_payout - total_active_stake + total_realized_profit
    
    return total_active_stake, total_expected_payout, total_realized_profit, total_ev

# ─────────────────  HISTORICAL EV PAGE  ────────────────
def historical_ev_page():
    try:
        # Attempt to connect to databases
        bet_conn = new_betting_conn()
        fut_conn = new_futures_conn()
        
        # Check if either connection failed
        if bet_conn is None or fut_conn is None:
            st.warning("⚠️ Unable to connect to one or more databases. Displaying demo data instead.")
            display_demo_data()
            return
        
        # Date range selection
        st.sidebar.header("Date Range")
        
        # Get the earliest bet date
        cursor = with_cursor(bet_conn)
        if cursor is None:
            st.error("Failed to create cursor for betting database")
            display_demo_data()
            return
            
        try:
            cursor.execute(
                """SELECT MIN(DateTimePlaced) as min_date 
                   FROM bets 
                   WHERE WhichBankroll='GreenAleph' AND WLCA IN ('Win', 'Loss', 'Cashout', 'Active')"""
            )
            result = cursor.fetchone()
            min_date = result["min_date"] if result and result["min_date"] else datetime(2023, 1, 1)
        except Exception as e:
            st.error(f"Error retrieving earliest bet date: {str(e)}")
            min_date = datetime(2023, 1, 1)
        
        now = datetime.utcnow()
        
        # Round dates to the nearest week
        min_date = min_date.replace(hour=0, minute=0, second=0, microsecond=0)
        min_date = min_date - timedelta(days=min_date.weekday())  # Previous Monday
        
        # Date range selection
        default_start = min_date
        default_end = now
        
        start_date = st.sidebar.date_input(
            "Start Date", 
            value=default_start.date(),
            min_value=min_date.date(),
            max_value=now.date()
        )
        
        end_date = st.sidebar.date_input(
            "End Date", 
            value=default_end.date(),
            min_value=start_date,
            max_value=now.date()
        )
        
        # Convert to datetime
        start_datetime = datetime.combine(start_date, datetime.min.time())
        end_datetime = datetime.combine(end_date, datetime.min.time())
        
        # Customize Vig
        st.sidebar.header("Customize Vig")
        default_vig = st.sidebar.slider(
            "Default Vig %", 
            min_value=0, 
            max_value=20, 
            value=5, 
            step=1
        )
        
        # Create vig map
        vig_map = {}
        unique_markets = sorted(set((et, el) for et, el in futures_table_map))
        with st.sidebar.expander("Set Custom Vig by Market", expanded=False):
            for et, el in unique_markets:
                key = f"{et}|{el}"
                percent = st.slider(
                    label=f"{et} — {el}", 
                    min_value=0, 
                    max_value=20,
                    value=default_vig, 
                    step=1, 
                    key=key
                )
                vig_map[(et, el)] = percent / 100.0
        
        # Generate weekly dates
        weekly_dates = get_weekly_dates(start_datetime, end_datetime)
        
        # Calculate progress
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        if st.button("Calculate Historical EV"):
            # Calculate EV for each date
            ev_data = []
            
            for i, date in enumerate(weekly_dates):
                # Update progress
                progress = int((i + 1) / len(weekly_dates) * 100)
                progress_bar.progress(progress)
                status_text.text(f"Calculating EV for {date.strftime('%Y-%m-%d')}... ({i+1}/{len(weekly_dates)})")
                
                active_stake, expected_payout, realized_profit, total_ev = get_ev_for_date(
                    date, bet_conn, fut_conn, vig_map
                )
                
                ev_data.append({
                    "Date": date,
                    "Active Stake": active_stake,
                    "Expected Payout": expected_payout,
                    "Realized Profit": realized_profit,
                    "Total EV": total_ev
                })
            
            # Clear progress indicators
            progress_bar.empty()
            status_text.empty()
            
            # Create DataFrame
            ev_df = pd.DataFrame(ev_data)
            
            # Plot data
            plot_historical_ev(ev_df)
            
            # Display data table
            with st.expander("View Data Table", expanded=False):
                st.dataframe(
                    ev_df.style.format({
                        "Active Stake": "${:,.2f}",
                        "Expected Payout": "${:,.2f}",
                        "Realized Profit": "${:,.2f}",
                        "Total EV": "${:,.2f}"
                    }),
                    use_container_width=True
                )
        else:
            st.info("Click 'Calculate Historical EV' to generate the chart.")
        
        # Close database connections
        if bet_conn:
            bet_conn.close()
        if fut_conn:
            fut_conn.close()
        
    except Exception as e:
        st.error(f"An unexpected error occurred: {str(e)}")
        st.exception(e)
        display_demo_data()

def plot_historical_ev(df):
    """Plot historical EV data"""
    st.header("Historical Expected Value")
    
    # Create figure
    fig = go.Figure()
    
    # Add Total EV line
    fig.add_trace(
        go.Scatter(
            x=df["Date"],
            y=df["Total EV"],
            mode="lines+markers",
            name="Total Expected Value",
            line=dict(width=3, color="green"),
            marker=dict(size=8)
        )
    )
    
    # Add Active Stake line
    fig.add_trace(
        go.Scatter(
            x=df["Date"],
            y=df["Active Stake"],
            mode="lines",
            name="Active Stake",
            line=dict(width=2, color="blue", dash="dot"),
            opacity=0.7
        )
    )
    
    # Add Expected Payout line
    fig.add_trace(
        go.Scatter(
            x=df["Date"],
            y=df["Expected Payout"],
            mode="lines",
            name="Expected Payout",
            line=dict(width=2, color="purple", dash="dot"),
            opacity=0.7
        )
    )
    
    # Add Realized Profit line
    fig.add_trace(
        go.Scatter(
            x=df["Date"],
            y=df["Realized Profit"],
            mode="lines",
            name="Realized Profit",
            line=dict(width=2, color="orange", dash="dot"),
            opacity=0.7
        )
    )
    
    # Update layout
    fig.update_layout(
        title="Weekly NBA Futures Expected Value",
        xaxis_title="Date",
        yaxis_title="Amount ($)",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        template="plotly_white",
        height=600
    )
    
    # Display chart
    st.plotly_chart(fig, use_container_width=True)
    
    # Add metrics
    if not df.empty:
        latest_ev = df.iloc[-1]["Total EV"]
        ev_change = latest_ev - df.iloc[0]["Total EV"] if len(df) > 1 else 0
        
        col1, col2, col3 = st.columns(3)
        
        col1.metric(
            "Current EV", 
            f"${latest_ev:,.2f}", 
            f"{ev_change:+,.2f}" if ev_change != 0 else "0.00"
        )
        
        col2.metric(
            "Max EV", 
            f"${df['Total EV'].max():,.2f}"
        )
        
        col3.metric(
            "Min EV", 
            f"${df['Total EV'].min():,.2f}"
        )

def display_demo_data():
    """Display demo data when database connection fails"""
    st.info("🔍 Displaying sample data for demonstration purposes. Connect to the database to see actual data.")
    
    # Create sample dates
    now = datetime.utcnow()
    dates = [now - timedelta(days=7*i) for i in range(20, -1, -1)]
    
    # Create synthetic EV data
    np.random.seed(42)
    
    # Parameters for a more realistic simulation
    stake_base = 5000  # starting stake amount
    stake_growth = 0.05  # weekly growth rate
    payout_ratio = 3.0  # expected payout as multiple of stake
    profit_ratio = 0.15  # realized profit as percentage of stake
    
    # Generate more realistic data
    ev_data = []
    for i, date in enumerate(dates):
        # Growing stake over time
        stake = stake_base * (1 + stake_growth * i)
        
        # Add some randomness
        stake_with_noise = max(0, stake * (1 + np.random.normal(0, 0.1)))
        expected_payout = stake_with_noise * payout_ratio * (1 + np.random.normal(0, 0.15))
        realized_profit = stake_with_noise * profit_ratio * (i/len(dates)) * (1 + np.random.normal(0, 0.5))
        
        # Calculate total EV
        total_ev = expected_payout - stake_with_noise + realized_profit
        
        ev_data.append({
            "Date": date,
            "Active Stake": stake_with_noise,
            "Expected Payout": expected_payout,
            "Realized Profit": realized_profit,
            "Total EV": total_ev
        })
    
    # Create DataFrame
    sample_df = pd.DataFrame(ev_data)
    
    # Plot the data
    plot_historical_ev(sample_df)
    
    # Display the data table
    with st.expander("View Sample Data", expanded=False):
        st.dataframe(
            sample_df.style.format({
                "Active Stake": "${:,.2f}",
                "Expected Payout": "${:,.2f}",
                "Realized Profit": "${:,.2f}",
                "Total EV": "${:,.2f}"
            }),
            use_container_width=True
        )
    
    st.warning("""
    ### Troubleshooting Database Connection Issues:
    
    1. **Check your database credentials** - Ensure username, password, and host are correct
    2. **Network connectivity** - Make sure your database is accessible from Streamlit Cloud:
       - AWS RDS databases need to allow inbound connections from Streamlit Cloud IP ranges
       - Consider using SSL for secure connections
    3. **Use Streamlit Secrets** - Store credentials securely using `st.secrets`
    4. **Check database status** - Verify your database instance is running and accessible
    
    For more help, check Streamlit's documentation on [connecting to databases](https://docs.streamlit.io/knowledge-base/tutorials/databases).
    """)

# Run the page
if __name__ == "__main__":
    historical_ev_page()
