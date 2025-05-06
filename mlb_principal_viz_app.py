# ─────────────────────  NBA Futures Dashboard: EV Table Page  ──────────────────────
import streamlit as st
import pandas as pd
import re
from collections import defaultdict
from datetime import datetime
import traceback

# Import pymysql with error handling
try:
    import pymysql
except ImportError:
    st.error("PyMySQL is not installed. Please install it with `pip install pymysql`")
    st.stop()

# ─────────────────  PAGE CONFIG  ─────────────────
st.set_page_config(page_title="NBA Futures EV Table", layout="wide")
st.markdown("<h1 style='text-align: center;'>NBA Futures EV Table</h1>", unsafe_allow_html=True)
st.markdown("<h3 style='text-align: center; color: gray;'>among markets tracked in <code>futures_db</code></h3>", unsafe_allow_html=True)

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
            st.info("If you're running on Streamlit Cloud, make sure your database is accessible from external networks or consider using Streamlit secrets for credentials.")
        
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
            st.info("If you're running on Streamlit Cloud, make sure your database is accessible from external networks or consider using Streamlit secrets for credentials.")
        
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

# ─────────────────  EV TABLE PAGE  ────────────────
def ev_table_page():
    try:
        # Attempt to connect to databases
        bet_conn = new_betting_conn()
        fut_conn = new_futures_conn()
        
        # Check if either connection failed
        if bet_conn is None or fut_conn is None:
            st.warning("⚠️ Unable to connect to one or more databases. Displaying demo data instead.")
            display_demo_data()
            return
            
        now = datetime.utcnow()

        # Customize Vig
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

        # ------- Active wagers -------
        sql_active = """
            SELECT b.WagerID, b.PotentialPayout, b.DollarsAtStake,
                l.EventType, l.EventLabel, l.ParticipantName
            FROM bets b JOIN legs l ON b.WagerID = l.WagerID
            WHERE b.WhichBankroll='GreenAleph' AND b.WLCA='Active' AND l.LeagueName='NBA'
        """
        
        cursor = with_cursor(bet_conn)
        if cursor is None:
            st.error("Failed to create cursor for betting database")
            display_demo_data()
            return
            
        try:
            cursor.execute(sql_active)
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

        # ------- Realised net profit -------
        sql_real = """
            SELECT b.WagerID, b.NetProfit,
                l.EventType, l.EventLabel, l.ParticipantName
            FROM bets b JOIN legs l ON b.WagerID = l.WagerID
            WHERE b.WhichBankroll='GreenAleph'
            AND b.WLCA IN ('Win','Loss','Cashout')
            AND l.LeagueName='NBA'
        """
        
        try:
            cursor.execute(sql_real)
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
            decs = [(best_odds_decimal_prob(et,el,pn,now,fut_conn,vig_inputs)[0], et, el) for et,el,pn in legs]
            sum_exc = sum(d-1 for d,_,_ in decs)
            if sum_exc <= 0: continue
            for d,et,el in decs:
                realized_np[(et,el)] += net * ((d-1)/sum_exc)

        # Close database connections
        if bet_conn:
            bet_conn.close()
        if fut_conn:
            fut_conn.close()

        # ------- Assemble dataframe -------
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

        display_data(df)
        
    except Exception as e:
        st.error(f"An unexpected error occurred: {str(e)}")
        st.code(traceback.format_exc())
        display_demo_data()

def display_data(df):
    """Display the data in a formatted way"""
    # ------- Summary Metrics -------
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("💸 Active Stake", f"${df['ActiveDollarsAtStake'].sum():,.0f}")
    col2.metric("📈 Expected Payout", f"${df['ActiveExpectedPayout'].sum():,.0f}")
    col3.metric("💰 Realized Net Profit", f"${df['RealizedNetProfit'].sum():,.0f}")
    col4.metric("⚡️ Expected Value", f"${df['ExpectedValue'].sum():,.0f}")

    # ------- Highlighted DataFrame -------
    def highlight_ev(val):
        color = "green" if val > 0 else "red" if val < 0 else "black"
        return f"color: {color}; font-weight: bold"

    styled_df = df.style.format("${:,.0f}", subset=[
        "ActiveDollarsAtStake", "ActiveExpectedPayout", "RealizedNetProfit", "ExpectedValue"]) \
        .applymap(highlight_ev, subset=["ExpectedValue"])

    st.markdown("### Market-Level Breakdown")
    st.dataframe(styled_df, use_container_width=True, height=700)

def display_demo_data():
    """Display demo data when database connection fails"""
    st.info("🔍 Displaying sample data for demonstration purposes. Connect to the database to see actual data.")
    
    # Create sample data
    sample_data = [
        {"EventType": "Championship", "EventLabel": "NBA Championship", 
         "ActiveDollarsAtStake": 5000, "ActiveExpectedPayout": 15000, 
         "RealizedNetProfit": 2000, "ExpectedValue": 12000},
        {"EventType": "Conference Winner", "EventLabel": "Eastern Conference", 
         "ActiveDollarsAtStake": 3000, "ActiveExpectedPayout": 9000, 
         "RealizedNetProfit": -500, "ExpectedValue": 5500},
        {"EventType": "Most Valuable Player Award", "EventLabel": "Award", 
         "ActiveDollarsAtStake": 2500, "ActiveExpectedPayout": 10000, 
         "RealizedNetProfit": 1200, "ExpectedValue": 8700},
    ]
    
    df = pd.DataFrame(sample_data)
    display_data(df)
    
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
    ev_table_page()
