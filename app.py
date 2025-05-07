import streamlit as st
import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from collections import defaultdict

# Retrieve secrets from Streamlit
db_host = st.secrets["DB_HOST"]
db_user = st.secrets["DB_USER"]
db_password = st.secrets["DB_PASSWORD"]
db_name = st.secrets["DB_NAME"]

# Function to get data from MySQL database
def get_data_from_db(query, params=None):
    try:
        conn = mysql.connector.connect(
            host=db_host,
            user=db_user,
            password=db_password,
            database=db_name
        )
        cursor = conn.cursor(dictionary=True)
        cursor.execute(query, params)
        data = cursor.fetchall()
        cursor.close()
        conn.close()
        return data
    except mysql.connector.Error as err:
        st.error(f"Error: {err}")
        return None

# Fetch the most recent update time
update_time_query = "SELECT MAX(DateTimePlaced) as LastUpdateTime FROM bets"
update_time_data = get_data_from_db(update_time_query)

if update_time_data:
    last_update_time = update_time_data[0]['LastUpdateTime']
else:
    last_update_time = "Unknown"

# Sidebar for navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["Main Page", "Principal Volume", "Betting Frequency", "NBA Charts", "NCAAB Charts", "NHL Charts", "NFL Charts", "NFL Playoffs EV", "Tennis Charts", "MLB Charts", "MLB Principal Tables", "NBA Participant Positions", "NFL Participant Positions"])


# Check if the user is on the "Main Page" page
if page == "Main Page":
    # Page title and update time display
    st.title('Principal Dashboard - GreenAleph I')
    st.markdown(f"**Last Update:** {last_update_time}", unsafe_allow_html=True)

    # SQL query for Active Principal by League bar chart
    data_query = """
    WITH DistinctBets AS (
        SELECT DISTINCT WagerID, DollarsAtStake, NetProfit
        FROM bets
        WHERE WhichBankroll = 'GreenAleph'
          AND WLCA = 'Active'
    )
    SELECT 
        l.LeagueName,
        ROUND(SUM(DollarsAtStake)) AS TotalDollarsAtStake
    FROM 
        DistinctBets db
    JOIN 
        (SELECT DISTINCT WagerID, LeagueName FROM legs) l ON db.WagerID = l.WagerID
    GROUP BY 
        l.LeagueName
    UNION ALL
    SELECT 
        'Total' AS LeagueName,
        ROUND(SUM(DollarsAtStake)) AS TotalDollarsAtStake
    FROM 
        DistinctBets;
    """

    # Fetch and process data for Active Principal by League
    active_principal_data = get_data_from_db(data_query)
    if active_principal_data is None:
        st.error("Failed to fetch active principal data from the database.")
    else:
        active_principal_df = pd.DataFrame(active_principal_data)
        active_principal_df['TotalDollarsAtStake'] = active_principal_df['TotalDollarsAtStake'].astype(float)
        active_principal_df = active_principal_df.sort_values(by='TotalDollarsAtStake')
        
        colors = ['#77dd77', '#89cff0', '#fdfd96', '#ffb347', '#aec6cf', '#cfcfc4', '#ffb6c1', '#b39eb5']
        total_color = 'lightblue'
        bar_colors = [total_color if name == 'Total' else colors[i % len(colors)] for i, name in enumerate(active_principal_df['LeagueName'])]
        
        fig, ax = plt.subplots(figsize=(15, 10))
        bars = ax.bar(active_principal_df['LeagueName'], active_principal_df['TotalDollarsAtStake'], color=bar_colors, width=0.6, edgecolor='black')
        ax.set_title('GA1: Total Active Principal', fontsize=18, fontweight='bold')
        ax.set_ylabel('Total Dollars At Stake ($)', fontsize=14, fontweight='bold')
        for bar in bars:
            height = bar.get_height()
            ax.annotate(f'${height:,.0f}', xy=(bar.get_x() + bar.get_width() / 2, height),
                        xytext=(0, 3), textcoords="offset points",
                        ha='center', va='bottom', fontsize=12, fontweight='bold', color='black')
        plt.xticks(rotation=45, ha='right', fontsize=14, fontweight='bold')
        ax.axhline(0, color='black', linewidth=0.8)
        ax.set_facecolor('white')
        for spine in ax.spines.values():
            spine.set_edgecolor('black')
            spine.set_linewidth(1.2)
        plt.tight_layout()
        st.pyplot(fig)

    # Add extra spacing
    st.markdown("<br><br><br>", unsafe_allow_html=True)

    # SQL query for Total Dollars Deployed
    deployed_query = """
    WITH ActiveBets AS (
        SELECT DollarsAtStake, NetProfit
        FROM bets
        WHERE WhichBankroll = 'GreenAleph'
          AND WLCA = 'Active'
    ),
    TotalBets AS (
        SELECT SUM(DollarsAtStake) AS TotalDollarsAtStake
        FROM ActiveBets
    ),
    TotalNetProfit AS (
        SELECT SUM(NetProfit) AS TotalNetProfit
        FROM bets
        WHERE WhichBankroll = 'GreenAleph'
    )
    SELECT 
        (TotalBets.TotalDollarsAtStake - COALESCE(TotalNetProfit.TotalNetProfit, 0)) AS TotalDollarsDeployed
    FROM 
        TotalBets, TotalNetProfit;
    """

    # Fetch and process data for Total Dollars Deployed
    deployed_data = get_data_from_db(deployed_query)
    if deployed_data and len(deployed_data) > 0 and 'TotalDollarsDeployed' in deployed_data[0]:
        total_dollars_deployed = deployed_data[0]['TotalDollarsDeployed']
        total_dollars_deployed = round(float(total_dollars_deployed)) if total_dollars_deployed is not None else 0
        total_dollars_deployed += 15000
        goal_amount = 500000
        progress_percentage = min(total_dollars_deployed / goal_amount, 1)
        label_position_percentage = progress_percentage * 50
        
        # Display Total $ Deployed progress bar between the charts
        st.markdown(f"<h4 style='text-align: center; font-weight: bold; color: black;'>Total $ Deployed (Total Active Principal - Realized Profit)</h4>", unsafe_allow_html=True)
        st.markdown(f"""
        <div style='width: 80%; margin: 0 auto;'>
            <div style='background-color: lightgray; height: 40px; position: relative; border-radius: 5px;'>
                <div style='background: linear-gradient(to right, lightblue {progress_percentage * 100}%, lightgray 0%); width: 100%; height: 100%; border-radius: 5px; position: relative;'>
                    <span style='position: absolute; left: {label_position_percentage}%; top: 50%; transform: translate(-50%, -50%); color: white; font-weight: bold;'>${total_dollars_deployed:,}</span>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        st.markdown(f"<h5 style='text-align: center; font-weight: bold; color: gray;'>$500k Initial Deployment Goal</h5>", unsafe_allow_html=True)
    else:
        st.error("No data available for Total Dollars Deployed.")

    # Add extra spacing
    st.markdown("<br><br><br>", unsafe_allow_html=True)

    # SQL query for Profit by League bar chart
    league_profit_query = """
    WITH DistinctBets AS (
        SELECT DISTINCT b.WagerID, b.NetProfit, l.LeagueName
        FROM bets b
        JOIN legs l ON b.WagerID = l.WagerID
        WHERE b.WhichBankroll = 'GreenAleph'
        AND b.LegCount = 1
    ),
    LeagueSums AS (
        SELECT 
            db.LeagueName,
            ROUND(SUM(db.NetProfit), 0) AS NetProfit
        FROM 
            DistinctBets db
        GROUP BY 
            db.LeagueName
    )
    SELECT * FROM LeagueSums
    UNION ALL
    SELECT 
        'Total' AS LeagueName,
        ROUND(SUM(b.NetProfit), 0) AS NetProfit
    FROM 
        bets b
    WHERE 
        b.WhichBankroll = 'GreenAleph'
    AND b.WagerID IN (SELECT DISTINCT WagerID FROM legs);
    """

    # Fetch and process data for Profit by League
    league_profit_data = get_data_from_db(league_profit_query)
    if league_profit_data is None:
        st.error("Failed to fetch league profit data from the database.")
    else:
        league_profit_df = pd.DataFrame(league_profit_data)
        league_profit_df['LeagueName'] = league_profit_df['LeagueName'].astype(str)
        league_profit_df['NetProfit'] = pd.to_numeric(league_profit_df['NetProfit'], errors='coerce')
        league_profit_df = league_profit_df.dropna(subset=['LeagueName', 'NetProfit'])

        if not league_profit_df.empty:
            fig, ax = plt.subplots(figsize=(15, 8))
            bar_colors = league_profit_df['NetProfit'].apply(lambda x: 'green' if x > 0 else 'red')
            bars = ax.bar(league_profit_df['LeagueName'], league_profit_df['NetProfit'], color=bar_colors, edgecolor='black')
            ax.set_title('Realized Profit by League', fontsize=18, fontweight='bold')
            ax.set_ylabel('Realized Profit ($)', fontsize=16, fontweight='bold')
            for bar in bars:
                height = bar.get_height()
                ax.annotate(f'${height:,.0f}', xy=(bar.get_x() + bar.get_width() / 2, height),
                            xytext=(0, 3), textcoords="offset points",
                            ha='center', va='bottom', fontsize=12, fontweight='bold', color='black')
            plt.xticks(rotation=45, ha='right', fontsize=14, fontweight='bold')
            ax.set_facecolor('white')
            for spine in ax.spines.values():
                spine.set_edgecolor('black')
                spine.set_linewidth(1.2)
            ymin = league_profit_df['NetProfit'].min() - 500
            ymax = league_profit_df['NetProfit'].max() + 1500
            ax.set_ylim(ymin, ymax)
            plt.tight_layout()
            st.pyplot(fig)

    # SQL query for Realized Profit by Month without excluding 'Cashout'
    monthly_profit_query = """
        SELECT 
            DATE_FORMAT(DateTimePlaced, '%Y-%m') AS Month,
            SUM(NetProfit) AS TotalNetProfit
        FROM bets
        WHERE WhichBankroll = 'GreenAleph'
        GROUP BY Month
        ORDER BY Month;
    """
    
    # Fetch and process data for Cumulative Realized Profit by Month
    monthly_profit_data = get_data_from_db(monthly_profit_query)
    if monthly_profit_data is None:
        st.error("Failed to fetch monthly realized profit data from the database.")
    else:
        # Convert data to DataFrame
        monthly_profit_df = pd.DataFrame(monthly_profit_data)
    
        # Ensure the DataFrame is not empty
        if not monthly_profit_df.empty:
            # Convert Month column to datetime and set as index
            monthly_profit_df['Month'] = pd.to_datetime(monthly_profit_df['Month'])
            monthly_profit_df.set_index('Month', inplace=True)
            monthly_profit_df.sort_index(inplace=True)
    
            # Calculate cumulative sum for the TotalNetProfit column
            monthly_profit_df['CumulativeNetProfit'] = monthly_profit_df['TotalNetProfit'].cumsum()
    
            # Determine y-axis limits with a buffer around min and max values
            y_min = monthly_profit_df['CumulativeNetProfit'].min() - 6000
            y_max = monthly_profit_df['CumulativeNetProfit'].max() + 6000
    
            # Plot the Cumulative Realized Profit by Month line graph
            #st.subheader("Cumulative Realized Profit by Month for 'GreenAleph'")
            fig, ax = plt.subplots(figsize=(14, 8))
    
            # Separate data for segments above and below zero
            months = monthly_profit_df.index.strftime('%Y-%m')
            cumulative_profits = monthly_profit_df['CumulativeNetProfit']
    
            # Plot segments of the line with color based on whether cumulative profit is above or below zero
            for i in range(1, len(cumulative_profits)):
                color = 'green' if cumulative_profits[i] >= 0 else 'red'
                ax.plot(months[i-1:i+1], cumulative_profits[i-1:i+1], color=color, linewidth=3)
    
            # Enhancing the plot aesthetics
            ax.set_ylabel('Cumulative Realized Profit ($)', fontsize=16, fontweight='bold')
            ax.set_title('Cumulative Realized Profit by Month', fontsize=20, fontweight='bold')
            ax.axhline(0, color='black', linewidth=1)  # Add horizontal line at y=0
            ax.set_ylim(y_min, y_max)  # Set y-axis limits
    
            # Set x-axis labels with rotation and larger font size
            plt.xticks(rotation=45, ha='right', fontsize=14, fontweight='bold')
            plt.yticks(fontsize=14, fontweight='bold')
    
            # Add only the final value label on the right side
            final_month = months[-1]
            final_profit = cumulative_profits.iloc[-1]
            ax.annotate(f"${final_profit:,.0f}", xy=(final_month, final_profit),
                        xytext=(0, 8), textcoords="offset points",
                        ha='center', fontsize=14, fontweight='bold', color='black')
    
            plt.tight_layout()
            st.pyplot(fig)
        else:
            st.warning("No data available for monthly cumulative realized profit.")










if page == "Principal Volume":
    st.title("Principal Volume (GA1)")

    # Define custom color mapping
    league_colors = {
        'ATP': 'green',
        'WTA': 'yellow',
        'NBA': 'darkorange',
        'NCAA Men\'s Basketball': 'lightcoral',  # Light orange
        'Olympics': 'black',
        'NFL': 'purple',
        'MLB': 'gray'
    }

    # SQL queries
    stacked_principal_volume_query = """
        WITH DistinctBets AS (
            SELECT DISTINCT WagerID, DollarsAtStake, DateTimePlaced
            FROM bets
            WHERE WLCA != 'Cashout'
              AND WhichBankroll = 'GreenAleph'
        ),
        MonthlySums AS (
            SELECT 
                DATE_FORMAT(db.DateTimePlaced, '%Y-%m') AS Month,
                l.LeagueName,
                SUM(db.DollarsAtStake) AS TotalDollarsAtStake
            FROM 
                DistinctBets db
            JOIN 
                (SELECT DISTINCT WagerID, LeagueName FROM legs) l ON db.WagerID = l.WagerID
            GROUP BY 
                Month, l.LeagueName
        )
        SELECT * FROM MonthlySums
        ORDER BY Month, LeagueName;
    """

    stacked_principal_volume_weekly_query = """
        WITH DistinctBets AS (
            SELECT DISTINCT WagerID, DollarsAtStake, DateTimePlaced
            FROM bets
            WHERE WLCA != 'Cashout'
              AND WhichBankroll = 'GreenAleph'
        ),
        WeeklySums AS (
            SELECT 
                STR_TO_DATE(CONCAT(YEAR(db.DateTimePlaced), ' ', WEEK(db.DateTimePlaced, 3), ' 1'), '%X %V %w') AS WeekStart, 
                l.LeagueName,
                SUM(db.DollarsAtStake) AS TotalDollarsAtStake
            FROM 
                DistinctBets db
            JOIN 
                (SELECT DISTINCT WagerID, LeagueName FROM legs) l ON db.WagerID = l.WagerID
            GROUP BY 
                WeekStart, l.LeagueName
        )
        SELECT * FROM WeeklySums
        ORDER BY WeekStart, LeagueName;
    """

    stacked_principal_volume_daily_query = """
        WITH DistinctBets AS (
            SELECT DISTINCT WagerID, DollarsAtStake, DateTimePlaced
            FROM bets
            WHERE WLCA != 'Cashout'
              AND WhichBankroll = 'GreenAleph'
        ),
        DailySums AS (
            SELECT 
                DATE_FORMAT(db.DateTimePlaced, '%Y-%m-%d') AS Day,
                l.LeagueName,
                SUM(db.DollarsAtStake) AS TotalDollarsAtStake
            FROM 
                DistinctBets db
            JOIN 
                (SELECT DISTINCT WagerID, LeagueName FROM legs) l ON db.WagerID = l.WagerID
            GROUP BY 
                Day, l.LeagueName
        )
        SELECT * FROM DailySums
        ORDER BY Day, LeagueName;
    """

    league_principal_volume_query = """
        WITH DistinctBets AS (
            SELECT DISTINCT WagerID, DollarsAtStake
            FROM bets
            WHERE WLCA != 'Cashout'
              AND WhichBankroll = 'GreenAleph'
        )
        SELECT 
            l.LeagueName,
            SUM(db.DollarsAtStake) AS TotalDollarsAtStake
        FROM 
            DistinctBets db
        JOIN 
            (SELECT DISTINCT WagerID, LeagueName FROM legs) l ON db.WagerID = l.WagerID
        GROUP BY l.LeagueName
        ORDER BY TotalDollarsAtStake DESC;
    """

    # Fetch data
    stacked_principal_volume_data = get_data_from_db(stacked_principal_volume_query)
    weekly_principal_volume_data = get_data_from_db(stacked_principal_volume_weekly_query)
    daily_principal_volume_data = get_data_from_db(stacked_principal_volume_daily_query)
    league_principal_volume_data = get_data_from_db(league_principal_volume_query)

    # Helper function to ensure data is numeric
    def ensure_numeric(df, column_list):
        for column in column_list:
            df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0)
        return df

    # Helper function to assign colors
    def assign_colors(columns):
        return [league_colors.get(col, 'blue') for col in columns]

    # Monthly plot
    if stacked_principal_volume_data:
        df_monthly = pd.DataFrame(stacked_principal_volume_data)
        if not df_monthly.empty:
            df_monthly = ensure_numeric(df_monthly, ['TotalDollarsAtStake'])
            df_pivot_monthly = df_monthly.pivot_table(
                index='Month',
                columns='LeagueName',
                values='TotalDollarsAtStake',
                aggfunc='sum'
            ).fillna(0)

            df_pivot_monthly.index = pd.to_datetime(df_pivot_monthly.index, format='%Y-%m', errors='coerce')
            df_pivot_monthly.sort_index(inplace=True)

            st.subheader("Principal Volume by Month")
            plt.figure(figsize=(12, 6))
            ax = df_pivot_monthly.plot(
                kind='bar', 
                stacked=True, 
                figsize=(12, 6), 
                color=assign_colors(df_pivot_monthly.columns)
            )

            plt.ylabel('Total Principal ($)')
            plt.title('Principal Volume by Month')
            plt.xticks(ticks=range(len(df_pivot_monthly.index)), labels=df_pivot_monthly.index.strftime('%Y-%m'), rotation=45, ha='right')
            plt.legend(title='LeagueName', bbox_to_anchor=(1.05, 1), loc='upper left')
            plt.tight_layout()
            st.pyplot(plt)
        else:
            st.warning("No data available for 'GreenAleph' principal volume by month.")
    else:
        st.error("Failed to retrieve monthly data from the database.")

    # Weekly plot
    if weekly_principal_volume_data:
        df_weekly = pd.DataFrame(weekly_principal_volume_data)
        if not df_weekly.empty:
            df_weekly = ensure_numeric(df_weekly, ['TotalDollarsAtStake'])
            df_pivot_weekly = df_weekly.pivot_table(
                index='WeekStart',
                columns='LeagueName',
                values='TotalDollarsAtStake',
                aggfunc='sum'
            ).fillna(0)
    
            # Ensure the index is datetime and sort
            df_pivot_weekly.index = pd.to_datetime(df_pivot_weekly.index, errors='coerce').strftime('%Y-%m')
            df_pivot_weekly.sort_index(inplace=True)
    
            st.subheader("Principal Volume by Week")
            plt.figure(figsize=(12, 6))
            ax = df_pivot_weekly.plot(
                kind='bar',
                stacked=True,
                figsize=(12, 6),
                color=assign_colors(df_pivot_weekly.columns)
            )
    
            plt.ylabel('Total Principal ($)')
            plt.title('Total Principal Volume by Week (Stacked by LeagueName)')
            plt.xticks(
                ticks=range(len(df_pivot_weekly.index)),
                labels=df_pivot_weekly.index,
                rotation=45,
                ha='right'
            )
            plt.legend(title='LeagueName', bbox_to_anchor=(1.05, 1), loc='upper left')
            plt.tight_layout()
            st.pyplot(plt)
        else:
            st.warning("No data available for 'GreenAleph' principal volume by week.")
    else:
        st.error("Failed to retrieve weekly data from the database.")


    # Daily plot
    if daily_principal_volume_data:
        df_daily = pd.DataFrame(daily_principal_volume_data)
        if not df_daily.empty:
            df_daily = ensure_numeric(df_daily, ['TotalDollarsAtStake'])
            df_pivot_daily = df_daily.pivot_table(
                index='Day',
                columns='LeagueName',
                values='TotalDollarsAtStake',
                aggfunc='sum'
            ).fillna(0)

            df_pivot_daily.index = pd.to_datetime(df_pivot_daily.index, format='%Y-%m-%d', errors='coerce')
            df_pivot_daily.sort_index(inplace=True)

            st.subheader("Total Principal Volume by Day (Stacked by LeagueName)")
            plt.figure(figsize=(12, 6))
            ax = df_pivot_daily.plot(
                kind='bar', 
                stacked=True, 
                figsize=(12, 6), 
                color=assign_colors(df_pivot_daily.columns)
            )

            plt.ylabel('Total Principal ($)')
            plt.title('Total Principal Volume by Day (Stacked by LeagueName)')
            monthly_labels = [label if i % 30 == 0 else '' for i, label in enumerate(df_pivot_daily.index.strftime('%Y-%m-%d'))]
            plt.xticks(ticks=range(len(df_pivot_daily.index)), labels=monthly_labels, rotation=45, ha='right')
            plt.legend(title='LeagueName', bbox_to_anchor=(1.05, 1), loc='upper left')
            plt.tight_layout()
            st.pyplot(plt)
        else:
            st.warning("No data available for 'GreenAleph' principal volume by day.")
    else:
        st.error("Failed to retrieve daily data from the database.")

    # Principal Volume by League plot
    if league_principal_volume_data:
        df_league = pd.DataFrame(league_principal_volume_data)
        if not df_league.empty:
            # Ensure the data is numeric and sort in ascending order
            df_league = ensure_numeric(df_league, ['TotalDollarsAtStake'])
            df_league = df_league.sort_values(by='TotalDollarsAtStake', ascending=True)
    
            st.subheader("Principal Volume by League")
            plt.figure(figsize=(12, 6))
            bar_colors = [league_colors.get(league, 'blue') for league in df_league['LeagueName']]
            plt.bar(df_league['LeagueName'], df_league['TotalDollarsAtStake'], color=bar_colors, edgecolor='black')
    
            plt.ylabel('Total Principal ($)')
            plt.title('Total Principal Volume by LeagueName')
            plt.xticks(rotation=45, ha='right')
    
            st.pyplot(plt)
        else:
            st.warning("No data available for 'GreenAleph' principal volume by league.")
    else:
        st.error("Failed to retrieve league data from the database.")








# Adding the new "Betting Frequency" page
if page == "Betting Frequency":
    st.title("Betting Frequency (GA1)")

    # SQL query to get the number of bets by month for 'GreenAleph'
    frequency_query = """
        SELECT 
            DATE_FORMAT(DateTimePlaced, '%Y-%m') AS Month,
            COUNT(WagerID) AS NumberOfBets
        FROM bets
        WHERE WhichBankroll = 'GreenAleph'
        GROUP BY Month
        ORDER BY Month;
    """

    # SQL query to get the total betting frequency by LeagueName for 'GreenAleph', counting each WagerID only once
    league_frequency_query = """
        SELECT 
            l.LeagueName,
            COUNT(DISTINCT b.WagerID) AS NumberOfBets
        FROM bets b
        JOIN legs l ON b.WagerID = l.WagerID
        WHERE b.WhichBankroll = 'GreenAleph'
        GROUP BY l.LeagueName
        ORDER BY NumberOfBets DESC;
    """

    # Get data from the database for the first chart
    frequency_data = get_data_from_db(frequency_query)

    if frequency_data:
        # Convert the data to a DataFrame for plotting
        df_frequency = pd.DataFrame(frequency_data)

        # Check if the DataFrame is not empty
        if not df_frequency.empty:
            df_frequency['Month'] = pd.to_datetime(df_frequency['Month'])
            df_frequency.set_index('Month', inplace=True)
            df_frequency.sort_index(inplace=True)

            # Calculate the total number of bets
            total_bets = df_frequency['NumberOfBets'].sum()
            total_row = pd.DataFrame({'NumberOfBets': [total_bets]}, index=['Total'])
            df_frequency = pd.concat([df_frequency, total_row])

            # Prepare x-axis labels
            x_labels = [date.strftime('%Y-%m') if isinstance(date, pd.Timestamp) else date for date in df_frequency.index]

            # Plot the first bar chart
            st.subheader("Number of Bets Placed by Month")
            plt.figure(figsize=(12, 6))
            bars = plt.bar(x_labels, df_frequency['NumberOfBets'])
            plt.ylabel('Number of Bets')
            plt.title('Number of Bets Placed by Month (GreenAleph)')
            plt.xticks(rotation=45, ha='right')

            # Add value labels above each bar
            for bar in bars:
                yval = bar.get_height()
                plt.text(bar.get_x() + bar.get_width()/2, yval, int(yval), ha='center', va='bottom')

            st.pyplot(plt)
        else:
            st.warning("No data available for 'GreenAleph' betting frequency.")
    else:
        st.error("Failed to retrieve data from the database.")

    # Get data from the database for the second chart
    league_frequency_data = get_data_from_db(league_frequency_query)

    if league_frequency_data:
        # Convert the data to a DataFrame for plotting
        df_league_frequency = pd.DataFrame(league_frequency_data)

        # Ensure correct data types for plotting
        df_league_frequency['LeagueName'] = df_league_frequency['LeagueName'].astype(str)
        df_league_frequency['NumberOfBets'] = df_league_frequency['NumberOfBets'].astype(int)

        # Sort by NumberOfBets in ascending order
        df_league_frequency = df_league_frequency.sort_values(by='NumberOfBets', ascending=True)

        # Check if the DataFrame is not empty
        if not df_league_frequency.empty:
            # Plot the second bar chart
            st.subheader("Betting Frequency by League")
            plt.figure(figsize=(12, 6))
            plt.bar(df_league_frequency['LeagueName'], df_league_frequency['NumberOfBets'])
            plt.ylabel('Number of Bets')
            plt.title('Number of Bets Placed by League (GreenAleph)')
            plt.xticks(rotation=45, ha='right')

            # Add value labels above each bar
            for index, value in enumerate(df_league_frequency['NumberOfBets']):
                plt.text(index, value, int(value), ha='center', va='bottom')

            st.pyplot(plt)
        else:
            st.warning("No data available for 'GreenAleph' betting frequency by league.")
    else:
        st.error("Failed to retrieve data from the database.")







elif page == "NBA Charts":
    # NBA Charts
    st.title('NBA Active Bets - GA1')

    # --- teams to EXCLUDE for “Championship” / “Conference Winner” ---
    EXCLUDED_NBA_TEAMS = [
        'Brooklyn Nets', 'Chicago Bulls', 'Dallas Mavericks',
        'New Orleans Pelicans', 'Philadelphia 76ers', 'Phoenix Suns',
        'Portland Trail Blazers', 'Sacramento Kings', 'San Antonio Spurs',
        'Toronto Raptors', 'Utah Jazz', 'Washington Wizards',
        'Atlanta Hawks', 'Charlotte Hornets', 'Los Angeles Clippers', 'Milwaukee Bucks',
        'Memphis Grizzlies', 'Orlando Magic', 'Miami Heat', 'Detroit Pistons'
    ]

    # 1️⃣  Active-principal by EventType  ───────────────────────────
    first_chart_query = """
    WITH DistinctBets AS (
        SELECT DISTINCT WagerID, DollarsAtStake
        FROM bets
        WHERE WhichBankroll = 'GreenAleph'
          AND WLCA = 'Active'
    ),
    EventTypeSums AS (
        SELECT l.EventType,
               ROUND(SUM(db.DollarsAtStake), 0) AS TotalDollarsAtStake
        FROM DistinctBets db
        JOIN (SELECT DISTINCT WagerID, EventType, LeagueName
              FROM legs) l ON db.WagerID = l.WagerID
        WHERE l.LeagueName = 'NBA'
        GROUP BY l.EventType
    )
    SELECT * FROM EventTypeSums
    UNION ALL
    SELECT 'Total' AS EventType,
           ROUND(SUM(db.DollarsAtStake), 0) AS TotalDollarsAtStake
    FROM DistinctBets db
    JOIN (SELECT DISTINCT WagerID, LeagueName
          FROM legs) l ON db.WagerID = l.WagerID
    WHERE l.LeagueName = 'NBA';
    """
    first_chart_df = pd.DataFrame(get_data_from_db(first_chart_query))
    first_chart_df['TotalDollarsAtStake'] = first_chart_df['TotalDollarsAtStake'].astype(float).round(0)
    first_chart_df = first_chart_df.sort_values('TotalDollarsAtStake')

    pastel = ['#a0d8f1', '#f4a261', '#e76f51', '#8ecae6', '#219ebc',
              '#023047', '#ffb703', '#fb8500', '#d4a5a5', '#9ab0a8']
    fig, ax = plt.subplots(figsize=(15, 10))
    ax.bar(first_chart_df['EventType'], first_chart_df['TotalDollarsAtStake'],
           color=[pastel[i % len(pastel)] for i in range(len(first_chart_df))],
           width=0.6, edgecolor='black')
    ax.set_title('Active Principal by EventType (GA1)', fontsize=18, fontweight='bold')
    ax.set_ylabel('Total Dollars At Stake ($)', fontsize=14, fontweight='bold')
    for bar in ax.patches:
        ax.annotate(f'{bar.get_height():,.0f}',
                    (bar.get_x() + bar.get_width()/2, bar.get_height()),
                    xytext=(0, 3), textcoords='offset points',
                    ha='center', va='bottom', fontsize=14, fontweight='bold')
    plt.xticks(rotation=45, ha='right', fontsize=14, fontweight='bold')
    ax.axhline(0, color='black', linewidth=0.8)
    for s in ax.spines.values():
        s.set_edgecolor('black'); s.set_linewidth(1.2)
    st.pyplot(fig)

    # 2️⃣  Dropdowns  ───────────────────────────────────────────────
    event_type_option = st.selectbox(
        'Select EventType',
        sorted(first_chart_df[first_chart_df['EventType'] != 'Total']['EventType'].unique())
    )
    if not event_type_option:
        st.stop()

    event_label_query = f"""
    SELECT DISTINCT l.EventLabel
    FROM bets b
    JOIN legs l ON b.WagerID = l.WagerID
    WHERE l.LeagueName = 'NBA'
      AND l.EventType = '{event_type_option}'
      AND b.WhichBankroll = 'GreenAleph'
      AND b.WLCA = 'Active';
    """
    event_label_option = st.selectbox(
        'Select EventLabel',
        sorted(row['EventLabel'] for row in get_data_from_db(event_label_query))
    )
    if not event_label_option:
        st.stop()

    # 3️⃣  Active-principal & potential-payout chart (straight bets) ─
    combined_query = f"""
    WITH DistinctBets AS (
        SELECT DISTINCT WagerID, DollarsAtStake, PotentialPayout
        FROM bets
        WHERE WhichBankroll = 'GreenAleph'
          AND WLCA = 'Active'
          AND LegCount = 1
    )
    SELECT l.ParticipantName,
           SUM(db.DollarsAtStake)  AS TotalDollarsAtStake,
           SUM(db.PotentialPayout) AS TotalPotentialPayout
    FROM DistinctBets db
    JOIN legs l ON db.WagerID = l.WagerID
    WHERE l.LeagueName = 'NBA'
      AND l.EventType = '{event_type_option}'
      AND l.EventLabel = '{event_label_option}'
    GROUP BY l.ParticipantName;
    """
    combined_df = pd.DataFrame(get_data_from_db(combined_query))

    # ▸ exclude unwanted teams for title / conference markets
    if event_type_option in ('Championship', 'Conference Winner'):
        combined_df = combined_df[~combined_df['ParticipantName'].isin(EXCLUDED_NBA_TEAMS)]

    if combined_df.empty:
        st.warning("No data for selected filters.")
        st.stop()

    combined_df['ImpliedProbability'] = (
        combined_df['TotalDollarsAtStake'] / combined_df['TotalPotentialPayout']
    ) * 100

    # 🔧  cast BEFORE round to avoid TypeError
    combined_df['TotalDollarsAtStake']  = -combined_df['TotalDollarsAtStake'].astype(float).round(0)
    combined_df['TotalPotentialPayout'] =  combined_df['TotalPotentialPayout'].astype(float).round(0)
    combined_df = combined_df.sort_values('TotalDollarsAtStake')

    fig, ax = plt.subplots(figsize=(18, 12))
    bars1 = ax.bar(combined_df['ParticipantName'], combined_df['TotalDollarsAtStake'],
                   color='lightblue', width=0.4, edgecolor='black')
    bars2 = ax.bar(combined_df['ParticipantName'], combined_df['TotalPotentialPayout'],
                   color='beige', width=0.4, edgecolor='black')
    ax.set_ylabel('USD ($) in MM', fontsize=16, fontweight='bold')
    ax.set_title('Active Principal & Potential Payout (Straight Bets Only)',
                 fontsize=18, fontweight='bold')
    for i, bar1 in enumerate(bars1):
        ax.annotate(f"{combined_df.iloc[i]['ImpliedProbability']:.1f}%",
                    (bar1.get_x()+bar1.get_width()/2, bar1.get_height()),
                    xytext=(0, -15), textcoords='offset points',
                    ha='center', va='bottom', fontsize=12, fontweight='bold')
    for bar2 in bars2:
        ax.annotate(f"{bar2.get_height():,.0f}",
                    (bar2.get_x()+bar2.get_width()/2, bar2.get_height()),
                    xytext=(0, 3), textcoords='offset points',
                    ha='center', va='bottom', fontsize=12, fontweight='bold', rotation=45)
    plt.xticks(rotation=45, ha='right', fontsize=14, fontweight='bold')
    ax.legend([bars2, bars1], ['Potential Payout', 'Implied Probability (%)'])
    ax.axhline(0, color='black', linewidth=0.8)
    for s in ax.spines.values():
        s.set_edgecolor('black'); s.set_linewidth(1.2)
    ax.set_ylim(min(combined_df['TotalDollarsAtStake']) - 35000,
                max(combined_df['TotalPotentialPayout']) + 80000)
    st.pyplot(fig)

    # 4️⃣  Parlays section  ────────────────────────────────────────
    st.header("NBA Parlays - GA1")
    parlay_count_query = f"""
    SELECT l.ParticipantName,
           COUNT(DISTINCT b.WagerID) AS NumberOfParlays
    FROM bets b
    JOIN legs l ON b.WagerID = l.WagerID
    WHERE b.WhichBankroll = 'GreenAleph'
      AND b.WLCA = 'Active'
      AND b.LegCount > 1
      AND l.LeagueName = 'NBA'
      AND l.EventType = %s
    GROUP BY l.ParticipantName
    ORDER BY NumberOfParlays DESC;
    """
    parlay_df = pd.DataFrame(get_data_from_db(parlay_count_query, [event_type_option]))
    if event_type_option in ('Championship', 'Conference Winner'):
        parlay_df = parlay_df[~parlay_df['ParticipantName'].isin(EXCLUDED_NBA_TEAMS)]

    if not parlay_df.empty:
        st.subheader(f"Number of Parlays by Participant for {event_type_option}")
        fig, ax = plt.subplots(figsize=(14, 8))
        bars = ax.bar(parlay_df['ParticipantName'], parlay_df['NumberOfParlays'],
                      color='skyblue', edgecolor='black')
        ax.set_title(f"Parlay Involvement by Participant for {event_type_option} (GA1)",
                     fontsize=18, fontweight='bold')
        ax.set_ylabel("Number of Parlays", fontsize=14, fontweight='bold')
        plt.xticks(rotation=45, ha='right', fontsize=12, fontweight='bold')
        for bar in bars:
            ax.annotate(f"{bar.get_height()}",
                        (bar.get_x()+bar.get_width()/2, bar.get_height()),
                        xytext=(0, 5), textcoords='offset points',
                        ha='center', va='bottom', fontsize=12)
        ax.axhline(0, color='black', linewidth=0.8)
        for s in ax.spines.values():
            s.set_edgecolor('black'); s.set_linewidth(1.2)
        st.pyplot(fig)

    parlay_dollars_query = f"""
    SELECT l.ParticipantName,
           SUM(b.DollarsAtStake) AS TotalDollarsAtStake
    FROM bets b
    JOIN legs l ON b.WagerID = l.WagerID
    WHERE b.WhichBankroll = 'GreenAleph'
      AND b.WLCA = 'Active'
      AND b.LegCount > 1
      AND l.LeagueName = 'NBA'
      AND l.EventType = %s
    GROUP BY l.ParticipantName
    ORDER BY TotalDollarsAtStake DESC;
    """
    parlay_dollars_df = pd.DataFrame(get_data_from_db(parlay_dollars_query, [event_type_option]))
    if event_type_option in ('Championship', 'Conference Winner'):
        parlay_dollars_df = parlay_dollars_df[
            ~parlay_dollars_df['ParticipantName'].isin(EXCLUDED_NBA_TEAMS)
        ]

    if not parlay_dollars_df.empty:
        st.subheader(f"Total Dollars At Stake in Parlays by Participant for {event_type_option}")
        fig, ax = plt.subplots(figsize=(14, 8))
        bars = ax.bar(parlay_dollars_df['ParticipantName'],
                      parlay_dollars_df['TotalDollarsAtStake'],
                      color='lightblue', edgecolor='black')
        ax.set_title(f"Total Dollars At Stake in Parlays by Participant for {event_type_option} (GA1)",
                     fontsize=18, fontweight='bold')
        ax.set_ylabel("Total Dollars At Stake ($)", fontsize=14, fontweight='bold')
        plt.xticks(rotation=45, ha='right', fontsize=12, fontweight='bold')
        for bar in bars:
            ax.annotate(f"${bar.get_height():,.0f}",
                        (bar.get_x()+bar.get_width()/2, bar.get_height()),
                        xytext=(0, 5), textcoords='offset points',
                        ha='center', va='bottom', fontsize=12)
        ax.axhline(0, color='black', linewidth=0.8)
        for s in ax.spines.values():
            s.set_edgecolor('black'); s.set_linewidth(1.2)
        st.pyplot(fig)




elif page == "NCAAB Charts":
    # NCAAB Charts
    st.title('NCAAB Active Bets - GA1')

    # SQL query to fetch data for the first bar chart
    first_chart_query = """
    WITH DistinctBets AS (
        SELECT DISTINCT WagerID, DollarsAtStake
        FROM bets
        WHERE WhichBankroll = 'GreenAleph'
          AND WLCA = 'Active'
    ),
    EventTypeSums AS (
        SELECT 
            l.EventType,
            ROUND(SUM(db.DollarsAtStake), 0) AS TotalDollarsAtStake
        FROM 
            DistinctBets db
        JOIN 
            (SELECT DISTINCT WagerID, EventType, LeagueName FROM legs) l ON db.WagerID = l.WagerID
        WHERE
            l.LeagueName = 'NCAA Mens Basketball'
        GROUP BY 
            l.EventType
    )
    SELECT * FROM EventTypeSums

    UNION ALL

    SELECT 
        'Total' AS EventType,
        ROUND(SUM(db.DollarsAtStake), 0) AS TotalDollarsAtStake
    FROM 
        DistinctBets db
    JOIN 
        (SELECT DISTINCT WagerID, LeagueName FROM legs) l ON db.WagerID = l.WagerID
    WHERE
        l.LeagueName = 'NCAA Mens Basketball';
    """

    # Fetch the data for the first bar chart
    first_chart_data = get_data_from_db(first_chart_query)

    # Check if data is fetched successfully
    if first_chart_data is None:
        st.error("Failed to fetch data from the database.")
    else:
        # Create a DataFrame from the fetched data
        first_chart_df = pd.DataFrame(first_chart_data)

        # Display the fetched data
        first_chart_df['TotalDollarsAtStake'] = first_chart_df['TotalDollarsAtStake'].astype(float).round(0)

        # Sort the DataFrame by 'TotalDollarsAtStake' in ascending order
        first_chart_df = first_chart_df.sort_values('TotalDollarsAtStake', ascending=True)

        # Define pastel colors for the first chart
        pastel_colors = ['#a0d8f1', '#f4a261', '#e76f51', '#8ecae6', '#219ebc', '#023047', '#ffb703', '#fb8500', '#d4a5a5', '#9ab0a8']

        # Plot the first bar chart
        fig, ax = plt.subplots(figsize=(15, 10))
        bars = ax.bar(first_chart_df['EventType'], first_chart_df['TotalDollarsAtStake'], color=[pastel_colors[i % len(pastel_colors)] for i in range(len(first_chart_df['EventType']))], width=0.6, edgecolor='black')

        # Add labels and title
        ax.set_title('Active Principal by EventType (GA1)', fontsize=18, fontweight='bold')
        ax.set_ylabel('Total Dollars At Stake ($)', fontsize=14, fontweight='bold')

        # Annotate each bar with the value (no dollar sign)
        for bar in bars:
            height = bar.get_height()
            ax.annotate(f'{height:,.0f}', xy=(bar.get_x() + bar.get_width() / 2, height),
                        xytext=(0, 3), textcoords="offset points",
                        ha='center', va='bottom', fontsize=14, fontweight='bold', color='black')

        # Rotate the x-axis labels to 45 degrees
        plt.xticks(rotation=45, ha='right', fontsize=14, fontweight='bold')

        # Add horizontal line at y=0 for reference
        ax.axhline(0, color='black', linewidth=0.8)

        # Set background color to white
        ax.set_facecolor('white')

        # Add border around the plot
        for spine in ax.spines.values():
            spine.set_edgecolor('black')
            spine.set_linewidth(1.2)

        # Adjust layout
        plt.tight_layout()

        # Use Streamlit to display the first chart
        st.pyplot(fig)

        # Filter for EventType, sorted in alphabetical order
        event_type_option = st.selectbox('Select EventType', sorted(first_chart_df[first_chart_df['EventType'] != 'Total']['EventType'].unique()))

        if event_type_option:
            # SQL query to fetch data for the EventLabel dropdown, sorted alphabetically
            event_label_query = f"""
            SELECT DISTINCT l.EventLabel
            FROM 
                bets b
            JOIN 
                legs l ON b.WagerID = l.WagerID
            WHERE
                l.LeagueName = 'NCAA Mens Basketball'
                AND l.EventType = '{event_type_option}'
                AND b.WhichBankroll = 'GreenAleph'
                AND b.WLCA = 'Active'
                ;
            """

            # Fetch the EventLabel data
            event_label_data = get_data_from_db(event_label_query)

            if event_label_data is None:
                st.error("Failed to fetch data from the database.")
            else:
                event_labels = [row['EventLabel'] for row in event_label_data]
                event_label_option = st.selectbox('Select EventLabel', sorted(event_labels))

                if event_label_option:
                    # SQL query to fetch data for the combined bar chart (DollarsAtStake and PotentialPayout)
                    combined_query = f"""
                    WITH DistinctBets AS (
                        SELECT DISTINCT WagerID, DollarsAtStake, PotentialPayout
                        FROM bets
                        WHERE WhichBankroll = 'GreenAleph'
                          AND WLCA = 'Active'
                          AND LegCount = 1
                    )
                    SELECT 
                        l.ParticipantName,
                        SUM(db.DollarsAtStake) AS TotalDollarsAtStake,
                        SUM(db.PotentialPayout) AS TotalPotentialPayout
                    FROM 
                        DistinctBets db
                    JOIN 
                        legs l ON db.WagerID = l.WagerID
                    WHERE
                        l.LeagueName = 'NCAA Mens Basketball'
                        AND l.EventType = '{event_type_option}'
                        AND l.EventLabel = '{event_label_option}'
                    GROUP BY 
                        l.ParticipantName;
                    """
                
                    # Fetch the combined data
                    combined_data = get_data_from_db(combined_query)
                    
                    # Check if data is fetched successfully
                    if combined_data is None:
                        st.error("Failed to fetch data from the database.")
                    else:
                        # Create a DataFrame from the fetched data
                        combined_df = pd.DataFrame(combined_data)
                    
                        # Calculate Implied Probability
                        combined_df['ImpliedProbability'] = (combined_df['TotalDollarsAtStake'] / combined_df['TotalPotentialPayout']) * 100
                    
                        # Modify TotalDollarsAtStake for the chart (to show negative values)
                        combined_df['TotalDollarsAtStake'] = -combined_df['TotalDollarsAtStake'].astype(float).round(0)
                        combined_df['TotalPotentialPayout'] = combined_df['TotalPotentialPayout'].astype(float).round(0)
                    
                        # Sort the DataFrame by 'TotalDollarsAtStake' in ascending order
                        combined_df = combined_df.sort_values('TotalDollarsAtStake', ascending=True)
                    
                        # Define colors for DollarsAtStake and PotentialPayout
                        color_dollars_at_stake = 'lightblue'  # Light blue for DollarsAtStake
                        color_potential_payout = 'beige'  # Beige for PotentialPayout
                    
                        # Plot the combined bar chart
                        fig, ax = plt.subplots(figsize=(18, 12))
                    
                        # Plot TotalDollarsAtStake moving downward from the x-axis
                        bars1 = ax.bar(combined_df['ParticipantName'], combined_df['TotalDollarsAtStake'], 
                                       color=color_dollars_at_stake, width=0.4, edgecolor='black')
                    
                        # Plot TotalPotentialPayout moving upward from the x-axis
                        bars2 = ax.bar(combined_df['ParticipantName'], combined_df['TotalPotentialPayout'], 
                                       color=color_potential_payout, width=0.4, edgecolor='black')
                    
                        # Add labels and title
                        ax.set_ylabel('USD ($) in MM', fontsize=16, fontweight='bold')
                        ax.set_title(f'Active Principal & Potential Payout (Straight Bets Only)', fontsize=18, fontweight='bold')
                    
                        # Annotate Implied Probability on TotalDollarsAtStake bars
                        for i, bar1 in enumerate(bars1):
                            implied_prob = combined_df.iloc[i]['ImpliedProbability']
                            height = bar1.get_height()
                            ax.annotate(f'{implied_prob:.1f}%', xy=(bar1.get_x() + bar1.get_width() / 2, height),
                                        xytext=(0, -15),  # Move the labels further down below the bars
                                        textcoords="offset points",
                                        ha='center', va='bottom', fontsize=12, fontweight='bold', color='black')
                    
                        # Annotate TotalPotentialPayout above bars
                        for bar2 in bars2:
                            height2 = bar2.get_height()
                            ax.annotate(f'{height2:,.0f}', xy=(bar2.get_x() + bar2.get_width() / 2, height2),
                                        xytext=(0, 3), textcoords="offset points",
                                        ha='center', va='bottom', fontsize=12, fontweight='bold', color='black', rotation = 45)
                    
                        # Rotate x-axis labels to 45 degrees
                        plt.xticks(rotation=45, ha='right', fontsize=14, fontweight='bold')
                    
                        # Add legend
                        ax.legend([bars2, bars1], ['Potential Payout', 'Implied Probability (%)'])
                    
                        # Add horizontal line at y=0 for reference
                        ax.axhline(0, color='black', linewidth=0.8)
                    
                        # Set background color to white
                        ax.set_facecolor('white')
                    
                        # Add border around the plot
                        for spine in ax.spines.values():
                            spine.set_edgecolor('black')
                            spine.set_linewidth(1.2)
                    
                        # Extend y-axis range
                        ax.set_ylim(min(combined_df['TotalDollarsAtStake']) - 35000, max(combined_df['TotalPotentialPayout']) + 80000)
                    
                        # Adjust layout
                        plt.tight_layout()
                    
                        # Use Streamlit to display the combined chart
                        st.pyplot(fig)


            # Add a new section at the bottom for tracking the number of parlays by participant
            st.header("NCAAB Parlays - GA1")

            # SQL query to count the number of parlays each participant is involved in for the selected EventType
            parlay_count_query = f"""
            SELECT 
                l.ParticipantName,
                COUNT(DISTINCT b.WagerID) AS NumberOfParlays
            FROM 
                bets b
            JOIN 
                legs l ON b.WagerID = l.WagerID
            WHERE 
                b.WhichBankroll = 'GreenAleph'
                AND b.WLCA = 'Active'
                AND b.LegCount > 1  -- Only count parlays
                AND l.LeagueName = 'NCAA Mens Basketball'
                AND l.EventType = %s
            GROUP BY 
                l.ParticipantName
            ORDER BY 
                NumberOfParlays DESC;
            """

            # Fetch the data for the parlay counts
            parlay_data = get_data_from_db(parlay_count_query, [event_type_option])

            # Display the data if available
            if parlay_data is None:
                st.error("Failed to fetch parlay data from the database.")
            else:
                parlay_df = pd.DataFrame(parlay_data)

                if parlay_df.empty:
                    st.warning("No parlay data found for the selected EventType.")
                else:
                    # Plot the parlay count bar chart
                    st.subheader(f"Number of Parlays by Participant for {event_type_option}")
                    fig, ax = plt.subplots(figsize=(14, 8))
                    
                    # Plot bar chart for NumberOfParlays
                    bars = ax.bar(parlay_df['ParticipantName'], parlay_df['NumberOfParlays'], color='skyblue', edgecolor='black')
                    
                    # Set title and labels
                    ax.set_title(f"Parlay Involvement by Participant for {event_type_option} (GA1)", fontsize=18, fontweight='bold')
                    ax.set_ylabel("Number of Parlays", fontsize=14, fontweight='bold')
                    
                    # Rotate x-axis labels
                    plt.xticks(rotation=45, ha='right', fontsize=12, fontweight='bold')
                    
                    # Annotate each bar with the count of parlays
                    for bar in bars:
                        height = bar.get_height()
                        ax.annotate(f"{height}", xy=(bar.get_x() + bar.get_width() / 2, height),
                                    xytext=(0, 5), textcoords="offset points",
                                    ha='center', va='bottom', fontsize=12, color='black')

                    # Add horizontal line at y=0
                    ax.axhline(0, color='black', linewidth=0.8)

                    # Set background color to white
                    ax.set_facecolor('white')

                    # Add border around the plot
                    for spine in ax.spines.values():
                        spine.set_edgecolor('black')
                        spine.set_linewidth(1.2)

                    # Adjust layout
                    plt.tight_layout()

                    # Display the plot in Streamlit
                    st.pyplot(fig)

                    # Additional chart for Total Dollars At Stake associated with Parlays by Participant
                    # SQL query to fetch the sum of DollarsAtStake for parlays by participant
                    parlay_dollars_query = f"""
                    SELECT 
                        l.ParticipantName,
                        SUM(b.DollarsAtStake) AS TotalDollarsAtStake
                    FROM 
                        bets b
                    JOIN 
                        legs l ON b.WagerID = l.WagerID
                    WHERE 
                        b.WhichBankroll = 'GreenAleph'
                        AND b.WLCA = 'Active'
                        AND b.LegCount > 1  -- Only count parlays
                        AND l.LeagueName = 'NCAA Mens Basketball'
                        AND l.EventType = %s
                    GROUP BY 
                        l.ParticipantName
                    ORDER BY 
                        TotalDollarsAtStake DESC;
                    """

                    # Fetch the data for the total dollars at stake in parlays
                    parlay_dollars_data = get_data_from_db(parlay_dollars_query, [event_type_option])

                    # Display the data if available
                    if parlay_dollars_data is None:
                        st.error("Failed to fetch total dollars at stake in parlays data from the database.")
                    else:
                        parlay_dollars_df = pd.DataFrame(parlay_dollars_data)

                        if parlay_dollars_df.empty:
                            st.warning("No parlay dollar data found for the selected EventType.")
                        else:
                            # Plot the total dollars at stake bar chart
                            st.subheader(f"Total Dollars At Stake in Parlays by Participant for {event_type_option}")
                            fig, ax = plt.subplots(figsize=(14, 8))
                            
                            # Plot bar chart for TotalDollarsAtStake
                            bars = ax.bar(parlay_dollars_df['ParticipantName'], parlay_dollars_df['TotalDollarsAtStake'], color='lightblue', edgecolor='black')
                            
                            # Set title and labels
                            ax.set_title(f"Total Dollars At Stake in Parlays by Participant for {event_type_option} (GA1)", fontsize=18, fontweight='bold')
                            ax.set_ylabel("Total Dollars At Stake ($)", fontsize=14, fontweight='bold')
                            
                            # Rotate x-axis labels
                            plt.xticks(rotation=45, ha='right', fontsize=12, fontweight='bold')
                            
                            # Annotate each bar with the dollar value
                            for bar in bars:
                                height = bar.get_height()
                                ax.annotate(f"${height:,.0f}", xy=(bar.get_x() + bar.get_width() / 2, height),
                                            xytext=(0, 5), textcoords="offset points",
                                            ha='center', va='bottom', fontsize=12, color='black')

                            # Add horizontal line at y=0
                            ax.axhline(0, color='black', linewidth=0.8)

                            # Set background color to white
                            ax.set_facecolor('white')

                            # Add border around the plot
                            for spine in ax.spines.values():
                                spine.set_edgecolor('black')
                                spine.set_linewidth(1.2)

                            # Adjust layout
                            plt.tight_layout()

                            # Display the plot in Streamlit
                            st.pyplot(fig)





elif page == "NHL Charts":
    # NHL Charts
    st.title('NHL Active Bets - GA1')

    # SQL query to fetch data for the first bar chart
    first_chart_query = """
    WITH DistinctBets AS (
        SELECT DISTINCT WagerID, DollarsAtStake
        FROM bets
        WHERE WhichBankroll = 'GreenAleph'
          AND WLCA = 'Active'
    ),
    EventTypeSums AS (
        SELECT 
            l.EventType,
            ROUND(SUM(db.DollarsAtStake), 0) AS TotalDollarsAtStake
        FROM 
            DistinctBets db
        JOIN 
            (SELECT DISTINCT WagerID, EventType, LeagueName FROM legs) l ON db.WagerID = l.WagerID
        WHERE
            l.LeagueName = 'NHL'
        GROUP BY 
            l.EventType
    )
    SELECT * FROM EventTypeSums

    UNION ALL

    SELECT 
        'Total' AS EventType,
        ROUND(SUM(db.DollarsAtStake), 0) AS TotalDollarsAtStake
    FROM 
        DistinctBets db
    JOIN 
        (SELECT DISTINCT WagerID, LeagueName FROM legs) l ON db.WagerID = l.WagerID
    WHERE
        l.LeagueName = 'NHL';
    """

    # Fetch the data for the first bar chart
    first_chart_data = get_data_from_db(first_chart_query)

    # Check if data is fetched successfully
    if first_chart_data is None:
        st.error("Failed to fetch data from the database.")
    else:
        # Create a DataFrame from the fetched data
        first_chart_df = pd.DataFrame(first_chart_data)

        # Display the fetched data
        first_chart_df['TotalDollarsAtStake'] = first_chart_df['TotalDollarsAtStake'].astype(float).round(0)

        # Sort the DataFrame by 'TotalDollarsAtStake' in ascending order
        first_chart_df = first_chart_df.sort_values('TotalDollarsAtStake', ascending=True)

        # Define pastel colors for the first chart
        pastel_colors = ['#a0d8f1', '#f4a261', '#e76f51', '#8ecae6', '#219ebc', '#023047', '#ffb703', '#fb8500', '#d4a5a5', '#9ab0a8']

        # Plot the first bar chart
        fig, ax = plt.subplots(figsize=(15, 10))
        bars = ax.bar(first_chart_df['EventType'], first_chart_df['TotalDollarsAtStake'], color=[pastel_colors[i % len(pastel_colors)] for i in range(len(first_chart_df['EventType']))], width=0.6, edgecolor='black')

        # Add labels and title
        ax.set_title('Active Principal by EventType (GA1)', fontsize=18, fontweight='bold')
        ax.set_ylabel('Total Dollars At Stake ($)', fontsize=14, fontweight='bold')

        # Annotate each bar with the value (no dollar sign)
        for bar in bars:
            height = bar.get_height()
            ax.annotate(f'{height:,.0f}', xy=(bar.get_x() + bar.get_width() / 2, height),
                        xytext=(0, 3), textcoords="offset points",
                        ha='center', va='bottom', fontsize=14, fontweight='bold', color='black')

        # Rotate the x-axis labels to 45 degrees
        plt.xticks(rotation=45, ha='right', fontsize=14, fontweight='bold')

        # Add horizontal line at y=0 for reference
        ax.axhline(0, color='black', linewidth=0.8)

        # Set background color to white
        ax.set_facecolor('white')

        # Add border around the plot
        for spine in ax.spines.values():
            spine.set_edgecolor('black')
            spine.set_linewidth(1.2)

        # Adjust layout
        plt.tight_layout()

        # Use Streamlit to display the first chart
        st.pyplot(fig)

        # Filter for EventType, sorted in alphabetical order
        event_type_option = st.selectbox('Select EventType', sorted(first_chart_df[first_chart_df['EventType'] != 'Total']['EventType'].unique()))

        if event_type_option:
            # SQL query to fetch data for the EventLabel dropdown, sorted alphabetically
            event_label_query = f"""
            SELECT DISTINCT l.EventLabel
            FROM 
                bets b
            JOIN 
                legs l ON b.WagerID = l.WagerID
            WHERE
                l.LeagueName = 'NHL'
                AND l.EventType = '{event_type_option}'
                AND b.WhichBankroll = 'GreenAleph'
                AND b.WLCA = 'Active'
                ;
            """

            # Fetch the EventLabel data
            event_label_data = get_data_from_db(event_label_query)

            if event_label_data is None:
                st.error("Failed to fetch data from the database.")
            else:
                event_labels = [row['EventLabel'] for row in event_label_data]
                event_label_option = st.selectbox('Select EventLabel', sorted(event_labels))

                if event_label_option:
                    # SQL query to fetch data for the combined bar chart (DollarsAtStake and PotentialPayout)
                    combined_query = f"""
                    WITH DistinctBets AS (
                        SELECT DISTINCT WagerID, DollarsAtStake, PotentialPayout
                        FROM bets
                        WHERE WhichBankroll = 'GreenAleph'
                          AND WLCA = 'Active'
                          AND LegCount = 1
                    )
                    SELECT 
                        l.ParticipantName,
                        SUM(db.DollarsAtStake) AS TotalDollarsAtStake,
                        SUM(db.PotentialPayout) AS TotalPotentialPayout
                    FROM 
                        DistinctBets db
                    JOIN 
                        legs l ON db.WagerID = l.WagerID
                    WHERE
                        l.LeagueName = 'NHL'
                        AND l.EventType = '{event_type_option}'
                        AND l.EventLabel = '{event_label_option}'
                    GROUP BY 
                        l.ParticipantName;
                    """
                
                    # Fetch the combined data
                    combined_data = get_data_from_db(combined_query)
                    
                    # Check if data is fetched successfully
                    if combined_data is None:
                        st.error("Failed to fetch data from the database.")
                    else:
                        # Create a DataFrame from the fetched data
                        combined_df = pd.DataFrame(combined_data)
                    
                        # Calculate Implied Probability
                        combined_df['ImpliedProbability'] = (combined_df['TotalDollarsAtStake'] / combined_df['TotalPotentialPayout']) * 100
                    
                        # Modify TotalDollarsAtStake for the chart (to show negative values)
                        combined_df['TotalDollarsAtStake'] = -combined_df['TotalDollarsAtStake'].astype(float).round(0)
                        combined_df['TotalPotentialPayout'] = combined_df['TotalPotentialPayout'].astype(float).round(0)
                    
                        # Sort the DataFrame by 'TotalDollarsAtStake' in ascending order
                        combined_df = combined_df.sort_values('TotalDollarsAtStake', ascending=True)
                    
                        # Define colors for DollarsAtStake and PotentialPayout
                        color_dollars_at_stake = 'lightblue'  # Light blue for DollarsAtStake
                        color_potential_payout = 'beige'  # Beige for PotentialPayout
                    
                        # Plot the combined bar chart
                        fig, ax = plt.subplots(figsize=(18, 12))
                    
                        # Plot TotalDollarsAtStake moving downward from the x-axis
                        bars1 = ax.bar(combined_df['ParticipantName'], combined_df['TotalDollarsAtStake'], 
                                       color=color_dollars_at_stake, width=0.4, edgecolor='black')
                    
                        # Plot TotalPotentialPayout moving upward from the x-axis
                        bars2 = ax.bar(combined_df['ParticipantName'], combined_df['TotalPotentialPayout'], 
                                       color=color_potential_payout, width=0.4, edgecolor='black')
                    
                        # Add labels and title
                        ax.set_ylabel('USD ($) in MM', fontsize=16, fontweight='bold')
                        ax.set_title(f'Active Principal & Potential Payout (Straight Bets Only)', fontsize=18, fontweight='bold')
                    
                        # Annotate Implied Probability on TotalDollarsAtStake bars
                        for i, bar1 in enumerate(bars1):
                            implied_prob = combined_df.iloc[i]['ImpliedProbability']
                            height = bar1.get_height()
                            ax.annotate(f'{implied_prob:.1f}%', xy=(bar1.get_x() + bar1.get_width() / 2, height),
                                        xytext=(0, -15),  # Move the labels further down below the bars
                                        textcoords="offset points",
                                        ha='center', va='bottom', fontsize=12, fontweight='bold', color='black')
                    
                        # Annotate TotalPotentialPayout above bars
                        for bar2 in bars2:
                            height2 = bar2.get_height()
                            ax.annotate(f'{height2:,.0f}', xy=(bar2.get_x() + bar2.get_width() / 2, height2),
                                        xytext=(0, 3), textcoords="offset points",
                                        ha='center', va='bottom', fontsize=12, fontweight='bold', color='black', rotation = 45)
                    
                        # Rotate x-axis labels to 45 degrees
                        plt.xticks(rotation=45, ha='right', fontsize=14, fontweight='bold')
                    
                        # Add legend
                        ax.legend([bars2, bars1], ['Potential Payout', 'Implied Probability (%)'])
                    
                        # Add horizontal line at y=0 for reference
                        ax.axhline(0, color='black', linewidth=0.8)
                    
                        # Set background color to white
                        ax.set_facecolor('white')
                    
                        # Add border around the plot
                        for spine in ax.spines.values():
                            spine.set_edgecolor('black')
                            spine.set_linewidth(1.2)
                    
                        # Extend y-axis range
                        ax.set_ylim(min(combined_df['TotalDollarsAtStake']) - 35000, max(combined_df['TotalPotentialPayout']) + 80000)
                    
                        # Adjust layout
                        plt.tight_layout()
                    
                        # Use Streamlit to display the combined chart
                        st.pyplot(fig)


            # Add a new section at the bottom for tracking the number of parlays by participant
            st.header("NHL Parlays - GA1")

            # SQL query to count the number of parlays each participant is involved in for the selected EventType
            parlay_count_query = f"""
            SELECT 
                l.ParticipantName,
                COUNT(DISTINCT b.WagerID) AS NumberOfParlays
            FROM 
                bets b
            JOIN 
                legs l ON b.WagerID = l.WagerID
            WHERE 
                b.WhichBankroll = 'GreenAleph'
                AND b.WLCA = 'Active'
                AND b.LegCount > 1  -- Only count parlays
                AND l.LeagueName = 'NHL'
                AND l.EventType = %s
            GROUP BY 
                l.ParticipantName
            ORDER BY 
                NumberOfParlays DESC;
            """

            # Fetch the data for the parlay counts
            parlay_data = get_data_from_db(parlay_count_query, [event_type_option])

            # Display the data if available
            if parlay_data is None:
                st.error("Failed to fetch parlay data from the database.")
            else:
                parlay_df = pd.DataFrame(parlay_data)

                if parlay_df.empty:
                    st.warning("No parlay data found for the selected EventType.")
                else:
                    # Plot the parlay count bar chart
                    st.subheader(f"Number of Parlays by Participant for {event_type_option}")
                    fig, ax = plt.subplots(figsize=(14, 8))
                    
                    # Plot bar chart for NumberOfParlays
                    bars = ax.bar(parlay_df['ParticipantName'], parlay_df['NumberOfParlays'], color='skyblue', edgecolor='black')
                    
                    # Set title and labels
                    ax.set_title(f"Parlay Involvement by Participant for {event_type_option} (GA1)", fontsize=18, fontweight='bold')
                    ax.set_ylabel("Number of Parlays", fontsize=14, fontweight='bold')
                    
                    # Rotate x-axis labels
                    plt.xticks(rotation=45, ha='right', fontsize=12, fontweight='bold')
                    
                    # Annotate each bar with the count of parlays
                    for bar in bars:
                        height = bar.get_height()
                        ax.annotate(f"{height}", xy=(bar.get_x() + bar.get_width() / 2, height),
                                    xytext=(0, 5), textcoords="offset points",
                                    ha='center', va='bottom', fontsize=12, color='black')

                    # Add horizontal line at y=0
                    ax.axhline(0, color='black', linewidth=0.8)

                    # Set background color to white
                    ax.set_facecolor('white')

                    # Add border around the plot
                    for spine in ax.spines.values():
                        spine.set_edgecolor('black')
                        spine.set_linewidth(1.2)

                    # Adjust layout
                    plt.tight_layout()

                    # Display the plot in Streamlit
                    st.pyplot(fig)

                    # Additional chart for Total Dollars At Stake associated with Parlays by Participant
                    # SQL query to fetch the sum of DollarsAtStake for parlays by participant
                    parlay_dollars_query = f"""
                    SELECT 
                        l.ParticipantName,
                        SUM(b.DollarsAtStake) AS TotalDollarsAtStake
                    FROM 
                        bets b
                    JOIN 
                        legs l ON b.WagerID = l.WagerID
                    WHERE 
                        b.WhichBankroll = 'GreenAleph'
                        AND b.WLCA = 'Active'
                        AND b.LegCount > 1  -- Only count parlays
                        AND l.LeagueName = 'NHL'
                        AND l.EventType = %s
                    GROUP BY 
                        l.ParticipantName
                    ORDER BY 
                        TotalDollarsAtStake DESC;
                    """

                    # Fetch the data for the total dollars at stake in parlays
                    parlay_dollars_data = get_data_from_db(parlay_dollars_query, [event_type_option])

                    # Display the data if available
                    if parlay_dollars_data is None:
                        st.error("Failed to fetch total dollars at stake in parlays data from the database.")
                    else:
                        parlay_dollars_df = pd.DataFrame(parlay_dollars_data)

                        if parlay_dollars_df.empty:
                            st.warning("No parlay dollar data found for the selected EventType.")
                        else:
                            # Plot the total dollars at stake bar chart
                            st.subheader(f"Total Dollars At Stake in Parlays by Participant for {event_type_option}")
                            fig, ax = plt.subplots(figsize=(14, 8))
                            
                            # Plot bar chart for TotalDollarsAtStake
                            bars = ax.bar(parlay_dollars_df['ParticipantName'], parlay_dollars_df['TotalDollarsAtStake'], color='lightblue', edgecolor='black')
                            
                            # Set title and labels
                            ax.set_title(f"Total Dollars At Stake in Parlays by Participant for {event_type_option} (GA1)", fontsize=18, fontweight='bold')
                            ax.set_ylabel("Total Dollars At Stake ($)", fontsize=14, fontweight='bold')
                            
                            # Rotate x-axis labels
                            plt.xticks(rotation=45, ha='right', fontsize=12, fontweight='bold')
                            
                            # Annotate each bar with the dollar value
                            for bar in bars:
                                height = bar.get_height()
                                ax.annotate(f"${height:,.0f}", xy=(bar.get_x() + bar.get_width() / 2, height),
                                            xytext=(0, 5), textcoords="offset points",
                                            ha='center', va='bottom', fontsize=12, color='black')

                            # Add horizontal line at y=0
                            ax.axhline(0, color='black', linewidth=0.8)

                            # Set background color to white
                            ax.set_facecolor('white')

                            # Add border around the plot
                            for spine in ax.spines.values():
                                spine.set_edgecolor('black')
                                spine.set_linewidth(1.2)

                            # Adjust layout
                            plt.tight_layout()

                            # Display the plot in Streamlit
                            st.pyplot(fig)













elif page == "NFL Charts":
    # NFL Charts
    st.title('NFL Active Bets - GA1')

    # SQL query to fetch data for the first bar chart
    first_chart_query = """
    WITH DistinctBets AS (
        SELECT DISTINCT WagerID, DollarsAtStake
        FROM bets
        WHERE WhichBankroll = 'GreenAleph'
          AND WLCA = 'Active'
    ),
    EventTypeSums AS (
        SELECT 
            l.EventType,
            ROUND(SUM(db.DollarsAtStake), 0) AS TotalDollarsAtStake
        FROM 
            DistinctBets db
        JOIN 
            (SELECT DISTINCT WagerID, EventType, LeagueName FROM legs) l ON db.WagerID = l.WagerID
        WHERE
            l.LeagueName = 'NFL'
        GROUP BY 
            l.EventType
    )
    SELECT * FROM EventTypeSums

    UNION ALL

    SELECT 
        'Total' AS EventType,
        ROUND(SUM(db.DollarsAtStake), 0) AS TotalDollarsAtStake
    FROM 
        DistinctBets db
    JOIN 
        (SELECT DISTINCT WagerID, LeagueName FROM legs) l ON db.WagerID = l.WagerID
    WHERE
        l.LeagueName = 'NFL';
    """

    # Fetch the data for the first bar chart
    first_chart_data = get_data_from_db(first_chart_query)

    # Check if data is fetched successfully
    if first_chart_data is None:
        st.error("Failed to fetch data from the database.")
    else:
        # Create a DataFrame from the fetched data
        first_chart_df = pd.DataFrame(first_chart_data)

        # Display the fetched data
        first_chart_df['TotalDollarsAtStake'] = first_chart_df['TotalDollarsAtStake'].astype(float).round(0)

        # Sort the DataFrame by 'TotalDollarsAtStake' in ascending order
        first_chart_df = first_chart_df.sort_values('TotalDollarsAtStake', ascending=True)

        # Define pastel colors for the first chart
        pastel_colors = ['#a0d8f1', '#f4a261', '#e76f51', '#8ecae6', '#219ebc', '#023047', '#ffb703', '#fb8500', '#d4a5a5', '#9ab0a8']

        # Plot the first bar chart
        fig, ax = plt.subplots(figsize=(15, 10))
        bars = ax.bar(first_chart_df['EventType'], first_chart_df['TotalDollarsAtStake'], color=[pastel_colors[i % len(pastel_colors)] for i in range(len(first_chart_df['EventType']))], width=0.6, edgecolor='black')

        # Add labels and title
        ax.set_title('Active Principal by EventType (GA1)', fontsize=18, fontweight='bold')
        ax.set_ylabel('Total Dollars At Stake ($)', fontsize=14, fontweight='bold')

        # Annotate each bar with the value (no dollar sign)
        for bar in bars:
            height = bar.get_height()
            ax.annotate(f'{height:,.0f}', xy=(bar.get_x() + bar.get_width() / 2, height),
                        xytext=(0, 3), textcoords="offset points",
                        ha='center', va='bottom', fontsize=14, fontweight='bold', color='black')

        # Rotate the x-axis labels to 45 degrees
        plt.xticks(rotation=45, ha='right', fontsize=14, fontweight='bold')

        # Add horizontal line at y=0 for reference
        ax.axhline(0, color='black', linewidth=0.8)

        # Set background color to white
        ax.set_facecolor('white')

        # Add border around the plot
        for spine in ax.spines.values():
            spine.set_edgecolor('black')
            spine.set_linewidth(1.2)

        # Adjust layout
        plt.tight_layout()

        # Use Streamlit to display the first chart
        st.pyplot(fig)

    # Fetch all unique EventType values from the database
    all_event_types_query = """
    SELECT DISTINCT EventType
    FROM legs
    WHERE LeagueName = 'NFL';
    """
    all_event_types_data = get_data_from_db(all_event_types_query)

    if all_event_types_data is None:
        st.error("Failed to fetch EventType data from the database.")
    else:
        all_event_types = [row['EventType'] for row in all_event_types_data]

        # Add a filter for WLCA status
        wlca_filter = st.radio(
            "Filter by Bet Status",
            options=["Active", "All"],
            index=0,  # Default to "Active"
            help="Choose whether to display Active bets only or include bets with Win, Loss, and Active statuses."
        )

        # Adjust the WLCA condition based on the filter
        wlca_condition = (
            "WLCA = 'Active'" if wlca_filter == "Active" else "WLCA IN ('Win', 'Loss', 'Active')"
        )

        # Filter for EventType
        event_type_option = st.selectbox('Select EventType', sorted(all_event_types))

        if event_type_option:
            # SQL query to calculate breakeven value
            breakeven_query = f"""
            SELECT
                -1 * SUM(CASE WHEN WLCA = 'Cashout' THEN NetProfit ELSE 0 END)
                + -1 * SUM(CASE WHEN WLCA = 'Loss' THEN NetProfit ELSE 0 END)
                + SUM(CASE WHEN WLCA = 'Active' THEN DollarsAtStake ELSE 0 END) AS Breakeven
            FROM bets b
            JOIN legs l ON b.WagerID = l.WagerID
            WHERE 
                b.WhichBankroll = 'GreenAleph'
                AND l.LeagueName = 'NFL'
                AND l.EventType = '{event_type_option}'
                AND b.LegCount = 1
            """

            # Fetch breakeven value
            breakeven_data = get_data_from_db(breakeven_query)
            breakeven_value = breakeven_data[0]['Breakeven'] if breakeven_data else 0

            # SQL query to fetch data for EventLabel dropdown
            event_label_query = f"""
            SELECT DISTINCT l.EventLabel
            FROM 
                bets b
            JOIN 
                legs l ON b.WagerID = l.WagerID
            WHERE
                l.LeagueName = 'NFL'
                AND l.EventType = '{event_type_option}'
                AND b.WhichBankroll = 'GreenAleph'
                AND {wlca_condition}
                ;
            """

            # Fetch EventLabel data
            event_label_data = get_data_from_db(event_label_query)
            if event_label_data is None:
                st.error("Failed to fetch EventLabel data from the database.")
            else:
                event_labels = [row['EventLabel'] for row in event_label_data]
                event_label_option = st.selectbox('Select EventLabel', sorted(event_labels))

                if event_label_option:
                    # Define the combined query with the adjusted WLCA condition
                    combined_query = f"""
                    WITH DistinctBets AS (
                        SELECT DISTINCT WagerID, DollarsAtStake, PotentialPayout
                        FROM bets
                        WHERE WhichBankroll = 'GreenAleph'
                          AND {wlca_condition}
                          AND LegCount = 1
                    )
                    SELECT 
                        l.ParticipantName,
                        SUM(db.DollarsAtStake) AS TotalDollarsAtStake,
                        SUM(db.PotentialPayout) AS TotalPotentialPayout
                    FROM 
                        DistinctBets db
                    JOIN 
                        legs l ON db.WagerID = l.WagerID
                    WHERE
                        l.LeagueName = 'NFL'
                        AND l.EventType = '{event_type_option}'
                        AND l.EventLabel = '{event_label_option}'
                    GROUP BY 
                        l.ParticipantName;
                    """

                    # Fetch the combined data
                    combined_data = get_data_from_db(combined_query)

                    # Check if data is fetched successfully
                    if combined_data is None:
                        st.error("Failed to fetch data from the database.")
                    else:
                        # Create a DataFrame from the fetched data
                        combined_df = pd.DataFrame(combined_data)

                        # Calculate Implied Probability
                        combined_df['ImpliedProbability'] = (combined_df['TotalDollarsAtStake'] / combined_df['TotalPotentialPayout']) * 100

                        # Modify TotalDollarsAtStake for the chart (to show negative values)
                        combined_df['TotalDollarsAtStake'] = -combined_df['TotalDollarsAtStake'].astype(float).round(0)
                                                # Sort the DataFrame by 'TotalDollarsAtStake' in ascending order
                        combined_df = combined_df.sort_values('TotalDollarsAtStake', ascending=True)

                        # Define colors for DollarsAtStake and PotentialPayout
                        color_dollars_at_stake = 'lightblue'  # Light blue for DollarsAtStake
                        color_potential_payout = 'orange'  # Orange for PotentialPayout

                        # Plot the combined bar chart
                        fig, ax = plt.subplots(figsize=(18, 12))

                        # Plot TotalDollarsAtStake moving downward from the x-axis
                        bars1 = ax.bar(combined_df['ParticipantName'], combined_df['TotalDollarsAtStake'],
                                       color=color_dollars_at_stake, width=0.4, edgecolor='black')

                        # Plot TotalPotentialPayout moving upward from the x-axis
                        bars2 = ax.bar(combined_df['ParticipantName'], combined_df['TotalPotentialPayout'],
                                       color=color_potential_payout, width=0.4, edgecolor='black')

                        # Add labels and title
                        ax.set_ylabel('USD ($)', fontsize=16, fontweight='bold')
                        ax.set_title(f'Active Principal & Potential Payout (Straight Bets Only) - {wlca_filter}', fontsize=18, fontweight='bold')

                        # Annotate Implied Probability on TotalDollarsAtStake bars
                        for i, bar1 in enumerate(bars1):
                            implied_prob = combined_df.iloc[i]['ImpliedProbability']
                            height = bar1.get_height()
                            ax.annotate(f'{implied_prob:.1f}%', xy=(bar1.get_x() + bar1.get_width() / 2, height),
                                        xytext=(0, -15),  # Move the labels further down below the bars
                                        textcoords="offset points",
                                        ha='center', va='bottom', fontsize=12, fontweight='bold', color='black')

                        # Annotate TotalPotentialPayout above bars
                        for bar2 in bars2:
                            height2 = bar2.get_height()
                            ax.annotate(f'{height2:,.0f}', xy=(bar2.get_x() + bar2.get_width() / 2, height2),
                                        xytext=(0, 3), textcoords="offset points",
                                        ha='center', va='bottom', fontsize=12, fontweight='bold', color='black')

                        # Rotate x-axis labels to 45 degrees
                        plt.xticks(rotation=45, ha='right', fontsize=14, fontweight='bold')

                        # Add legend
                        ax.legend([bars2, bars1], ['Potential Payout', 'Implied Probability (%)'])

                        # Add horizontal breakeven line
                        ax.axhline(breakeven_value, color='blue', linestyle='dashed', linewidth=1.5, label=f'Breakeven: ${breakeven_value:,.0f}')
                        ax.legend(loc='best', fontsize=18, title_fontsize=18)

                        # Add horizontal line at y=0 for reference
                        ax.axhline(0, color='black', linewidth=0.8)

                        # Set background color to white
                        ax.set_facecolor('white')

                        # Add border around the plot
                        for spine in ax.spines.values():
                            spine.set_edgecolor('black')
                            spine.set_linewidth(1.2)

                        # Extend y-axis range
                        ax.set_ylim(min(combined_df['TotalDollarsAtStake']) - 60000, max(combined_df['TotalPotentialPayout']) + 80000)

                        # Adjust layout
                        plt.tight_layout()

                        # Use Streamlit to display the combined chart
                        st.pyplot(fig)


    # Add a new section at the bottom for tracking NFL parlays
    st.header("NFL Parlays - GA1")
    
    # SQL query to count the number of parlays each participant is involved in for the selected EventType
    parlay_count_query = f"""
    SELECT 
        l.ParticipantName,
        COUNT(DISTINCT b.WagerID) AS NumberOfParlays
    FROM 
        bets b
    JOIN 
        legs l ON b.WagerID = l.WagerID
    WHERE 
        b.WhichBankroll = 'GreenAleph'
        AND b.WLCA = 'Active'
        AND b.LegCount > 1  -- Only count parlays
        AND l.LeagueName = 'NFL'
        AND l.EventType = %s
    GROUP BY 
        l.ParticipantName
    ORDER BY 
        NumberOfParlays DESC;
    """

    # Fetch the data for the parlay counts
    parlay_data = get_data_from_db(parlay_count_query, [event_type_option])

    # Display the data if available
    if parlay_data is None:
        st.error("Failed to fetch parlay data from the database.")
    else:
        parlay_df = pd.DataFrame(parlay_data)

        if parlay_df.empty:
            st.warning("No parlay data found for the selected EventType.")
        else:
            # Plot the parlay count bar chart
            st.subheader(f"Number of Parlays by Participant for {event_type_option}")
            fig, ax = plt.subplots(figsize=(14, 8))
            
            # Plot bar chart for NumberOfParlays
            bars = ax.bar(parlay_df['ParticipantName'], parlay_df['NumberOfParlays'], color='skyblue', edgecolor='black')
            
            # Set title and labels
            ax.set_title(f"Parlay Involvement by Participant for {event_type_option} (GA1)", fontsize=18, fontweight='bold')
            ax.set_ylabel("Number of Parlays", fontsize=14, fontweight='bold')
            
            # Rotate x-axis labels
            plt.xticks(rotation=45, ha='right', fontsize=12, fontweight='bold')
            
            # Annotate each bar with the count of parlays
            for bar in bars:
                height = bar.get_height()
                ax.annotate(f"{height}", xy=(bar.get_x() + bar.get_width() / 2, height),
                            xytext=(0, 5), textcoords="offset points",
                            ha='center', va='bottom', fontsize=12, color='black')

            # Add horizontal line at y=0
            ax.axhline(0, color='black', linewidth=0.8)

            # Set background color to white
            ax.set_facecolor('white')

            # Add border around the plot
            for spine in ax.spines.values():
                spine.set_edgecolor('black')
                spine.set_linewidth(1.2)

            # Adjust layout
            plt.tight_layout()

            # Display the plot in Streamlit
            st.pyplot(fig)

            # Additional chart for Total Dollars At Stake associated with Parlays by Participant
            parlay_dollars_query = f"""
            SELECT 
                l.ParticipantName,
                SUM(b.DollarsAtStake) AS TotalDollarsAtStake
            FROM 
                bets b
            JOIN 
                legs l ON b.WagerID = l.WagerID
            WHERE 
                b.WhichBankroll = 'GreenAleph'
                AND b.WLCA = 'Active'
                AND b.LegCount > 1  -- Only count parlays
                AND l.LeagueName = 'NFL'
                AND l.EventType = %s
            GROUP BY 
                l.ParticipantName
            ORDER BY 
                TotalDollarsAtStake DESC;
            """

            # Fetch the data for the total dollars at stake in parlays
            parlay_dollars_data = get_data_from_db(parlay_dollars_query, [event_type_option])

            # Display the data if available
            if parlay_dollars_data is None:
                st.error("Failed to fetch total dollars at stake in parlays data from the database.")
            else:
                parlay_dollars_df = pd.DataFrame(parlay_dollars_data)

                if parlay_dollars_df.empty:
                    st.warning("No parlay dollar data found for the selected EventType.")
                else:
                    # Plot the total dollars at stake bar chart
                    st.subheader(f"Total Dollars At Stake in Parlays by Participant for {event_type_option}")
                    fig, ax = plt.subplots(figsize=(14, 8))
                    
                    # Plot bar chart for TotalDollarsAtStake
                    bars = ax.bar(parlay_dollars_df['ParticipantName'], parlay_dollars_df['TotalDollarsAtStake'], color='lightblue', edgecolor='black')
                    
                    # Set title and labels
                    ax.set_title(f"Total Dollars At Stake in Parlays by Participant for {event_type_option} (GA1)", fontsize=18, fontweight='bold')
                    ax.set_ylabel("Total Dollars At Stake ($)", fontsize=14, fontweight='bold')
                    
                    # Rotate x-axis labels
                    plt.xticks(rotation=45, ha='right', fontsize=12, fontweight='bold')
                    
                    # Annotate each bar with the dollar value
                    for bar in bars:
                        height = bar.get_height()
                        ax.annotate(f"${height:,.0f}", xy=(bar.get_x() + bar.get_width() / 2, height),
                                    xytext=(0, 5), textcoords="offset points",
                                    ha='center', va='bottom', fontsize=12, color='black')

                    # Add horizontal line at y=0
                    ax.axhline(0, color='black', linewidth=0.8)

                    # Set background color to white
                    ax.set_facecolor('white')

                    # Add border around the plot
                    for spine in ax.spines.values():
                        spine.set_edgecolor('black')
                        spine.set_linewidth(1.2)

                    # Adjust layout
                    plt.tight_layout()

                    # Display the plot in Streamlit
                    st.pyplot(fig)










elif page == "NFL Playoffs EV":
    st.title("NFL Playoffs Expected Values")

    # Establish database connection
    conn = mysql.connector.connect(
        host='betting-db.cp86ssaw6cm7.us-east-1.rds.amazonaws.com',
        user='admin',
        password='7nRB1i2&A-K>',
        database='betting_db'
    )
    cursor = conn.cursor()

    # Query to fetch payouts
    query = """
        SELECT 
            legs.ParticipantName,
            legs.EventType,
            SUM(bets.PotentialPayout) AS total_payout
        FROM bets
        JOIN legs ON bets.WagerID = legs.WagerID
        WHERE 
            bets.LegCount = 1
            AND bets.WLCA = 'Active'
            AND bets.WhichBankroll = 'GreenAleph'
            AND legs.LeagueName = 'NFL'
            AND legs.EventType IN ('Conference Winner', 'Championship', 'Quarterfinals')
        GROUP BY legs.ParticipantName, legs.EventType;
    """
    cursor.execute(query)

    # Create payouts dictionary
    payouts = defaultdict(lambda: {'payout_conference': 0, 'payout_championship': 0, 'payout_quarterfinals': 0})
    for participant_name, event_type, total_payout in cursor.fetchall():
        normalized_name = participant_name.strip().lower()
        normalized_event_type = event_type.strip().lower().replace(" ", "_")
        payouts[normalized_name][f'payout_{normalized_event_type}'] = float(total_payout)

    # Close database connection
    cursor.close()
    conn.close()

    # Define matchups
    matchups = {
        "AFC": [
            ("Baltimore Ravens", "Buffalo Bills"),
            ("Houston Texans", "Kansas City Chiefs")
        ],
        "NFC": [
            ("Los Angeles Rams", "Philadelphia Eagles"), 
            ("Washington Commanders", "Detroit Lions")
        ]
    }

    # Team probabilities and payouts
    team_probabilities = {  # Must match lowercase team names
        "buffalo bills": {'current_round_prob': 0.483, 'current_quarterfinals_prob': 0.40, 'current_conference_prob': 0.27, 'current_champ_prob': 0.14},
        "baltimore ravens": {'current_round_prob': 0.517, 'current_quarterfinals_prob': 0.41, 'current_conference_prob': 0.30, 'current_champ_prob': 0.155},
        "kansas city chiefs": {'current_round_prob': 0.807, 'current_quarterfinals_prob': 0.40, 'current_conference_prob': 0.40, 'current_champ_prob': 0.205},
        "houston texans": {'current_round_prob': 0.193, 'current_quarterfinals_prob': 0.15, 'current_conference_prob': 0.03, 'current_champ_prob': 0.01},
        "detroit lions": {'current_round_prob': 0.813, 'current_quarterfinals_prob': 0.34, 'current_conference_prob': 0.475, 'current_champ_prob': 0.2425},
        "washington commanders": {'current_round_prob': 0.187, 'current_quarterfinals_prob': 0.12, 'current_conference_prob': 0.065, 'current_champ_prob': 0.0225},
        "philadelphia eagles": {'current_round_prob': 0.727, 'current_quarterfinals_prob': 0.34, 'current_conference_prob': 0.35, 'current_champ_prob': 0.18},
        "los angeles rams": {'current_round_prob': 0.273, 'current_quarterfinals_prob': 0.22, 'current_conference_prob': 0.11, 'current_champ_prob': 0.045}
    }

    # Normalize team names
    team_probabilities = {k.strip().lower(): v for k, v in team_probabilities.items()}

    # EV calculation functions
    def calculate_ev(probability, payout):
        return probability * payout

    def calculate_conditional_ev(current_quarterfinals_prob, quarterfinals_payout, current_conference_prob, conference_payout, current_champ_prob, champ_payout, current_round_prob):
        if current_round_prob > 0:
            conditional_quarterfinals_prob = current_quarterfinals_prob / current_round_prob
            conditional_conference_prob = current_conference_prob / current_round_prob
            conditional_champ_prob = current_champ_prob / current_round_prob
        else:
            return 0, 0, 0

        quarterfinals_ev = calculate_ev(conditional_quarterfinals_prob, quarterfinals_payout)
        conference_ev = calculate_ev(conditional_conference_prob, conference_payout)
        champ_ev = calculate_ev(conditional_champ_prob, champ_payout)

        return quarterfinals_ev, conference_ev, champ_ev

    # Loop through matchups and display results
    for conference, games in matchups.items():
        st.subheader(f"{conference} Conference Matchups")
        for team1_name, team2_name in games:
            team1_key = team1_name.strip().lower()
            team2_key = team2_name.strip().lower()

            st.write(f"### {team1_name} vs {team2_name}")

            # User input for probabilities
            st.write(f"Adjust probabilities for {team1_name}:")
            for key in ["current_round_prob", "current_quarterfinals_prob", "current_conference_prob", "current_champ_prob"]:
                team_probabilities[team1_key][key] = st.number_input(
                    f"{key.replace('_', ' ').capitalize()} for {team1_name}",
                    min_value=0.0, max_value=1.0, value=team_probabilities[team1_key][key]
                )

            st.write(f"Adjust probabilities for {team2_name}:")
            for key in ["current_round_prob", "current_quarterfinals_prob", "current_conference_prob", "current_champ_prob"]:
                team_probabilities[team2_key][key] = st.number_input(
                    f"{key.replace('_', ' ').capitalize()} for {team2_name}",
                    min_value=0.0, max_value=1.0, value=team_probabilities[team2_key][key]
                )

            # Calculate EVs
            team1_qf_ev, team1_cf_ev, team1_champ_ev = calculate_conditional_ev(
                team_probabilities[team1_key]["current_quarterfinals_prob"], payouts[team1_key]["payout_quarterfinals"],
                team_probabilities[team1_key]["current_conference_prob"], payouts[team1_key]["payout_conference"],
                team_probabilities[team1_key]["current_champ_prob"], payouts[team1_key]["payout_championship"],
                team_probabilities[team1_key]["current_round_prob"]
            )

            team2_qf_ev, team2_cf_ev, team2_champ_ev = calculate_conditional_ev(
                team_probabilities[team2_key]["current_quarterfinals_prob"], payouts[team2_key]["payout_quarterfinals"],
                team_probabilities[team2_key]["current_conference_prob"], payouts[team2_key]["payout_conference"],
                team_probabilities[team2_key]["current_champ_prob"], payouts[team2_key]["payout_championship"],
                team_probabilities[team2_key]["current_round_prob"]
            )

            team1_total_ev = team1_qf_ev + team1_cf_ev + team1_champ_ev
            team2_total_ev = team2_qf_ev + team2_cf_ev + team2_champ_ev

            # Display EVs
            st.write(f"**{team1_name} Total EV:** ${team1_total_ev:,.2f}")
            st.write(f"**{team2_name} Total EV:** ${team2_total_ev:,.2f}")

            # Plotting the EVs
            labels = ["Quarterfinals", "Conference", "Championship"]
            team1_evs = [team1_qf_ev, team1_cf_ev, team1_champ_ev]
            team2_evs = [team2_qf_ev, team2_cf_ev, team2_champ_ev]

            fig, ax = plt.subplots(figsize=(10, 6))
            bar_width = 0.35
            x = range(len(labels))

            ax.bar(x, team1_evs, width=bar_width, label=team1_name, color='blue')
            ax.bar([p + bar_width for p in x], team2_evs, width=bar_width, label=team2_name, color='orange')

            ax.set_xlabel("Stage", fontsize=12)
            ax.set_ylabel("Expected Value ($)", fontsize=12)
            ax.set_title(f"{team1_name} vs {team2_name} EV Breakdown", fontsize=16)
            ax.set_xticks([p + bar_width / 2 for p in x])
            ax.set_xticklabels(labels)
            ax.legend()

            st.pyplot(fig)

                



elif page == "Tennis Charts":
    
    st.title('Tennis Futures and Active Bets - GA1')

    # Function to fetch and plot bar charts
    def plot_bar_chart(data, title, ylabel):
        df = pd.DataFrame(data)
        df['TotalDollarsAtStake'] = df['TotalDollarsAtStake'].astype(float).round(0)
        df = df.sort_values('TotalDollarsAtStake', ascending=True)

        # Define pastel colors
        pastel_colors = ['#a0d8f1', '#f4a261', '#e76f51', '#8ecae6', '#219ebc', '#023047', '#ffb703', '#fb8500', '#d4a5a5', '#9ab0a8']

        fig, ax = plt.subplots(figsize=(15, 10))
        bars = ax.bar(df['EventLabel'], df['TotalDollarsAtStake'],
                      color=[pastel_colors[i % len(pastel_colors)] for i in range(len(df['EventLabel']))],
                      width=0.6, edgecolor='black')

        ax.set_title(title, fontsize=18, fontweight='bold')
        ax.set_ylabel(ylabel, fontsize=14, fontweight='bold')

        # Annotate each bar with the value
        for bar in bars:
            height = bar.get_height()
            ax.annotate(f'{height:,.0f}', xy=(bar.get_x() + bar.get_width() / 2, height),
                        xytext=(0, 3), textcoords="offset points",
                        ha='center', va='bottom', fontsize=14, fontweight='bold', color='black')

        plt.xticks(rotation=45, ha='right', fontsize=14, fontweight='bold')
        ax.axhline(0, color='black', linewidth=0.8)
        ax.set_facecolor('white')
        for spine in ax.spines.values():
            spine.set_edgecolor('black')
            spine.set_linewidth(1.2)
        plt.tight_layout()
        st.pyplot(fig)

    # Filter for LeagueName
    league_name = st.selectbox('Select League', ['ATP', 'WTA'])

    # SQL query for EventLabel breakdown (Futures)
    event_label_query = f"""
    WITH DistinctBets AS (
        SELECT DISTINCT WagerID, DollarsAtStake
        FROM bets
        WHERE WhichBankroll = 'GreenAleph'
          AND WLCA = 'Active'
          AND EXISTS (
              SELECT 1 
              FROM legs 
              WHERE legs.WagerID = bets.WagerID 
              AND legs.IsFuture = 'Yes'
          )
    )
    SELECT 
        l.EventLabel,
        ROUND(SUM(db.DollarsAtStake), 0) AS TotalDollarsAtStake
    FROM 
        DistinctBets db
    JOIN 
        legs l ON db.WagerID = l.WagerID
    WHERE
        l.LeagueName = '{league_name}'
        AND l.IsFuture = 'Yes'
    GROUP BY 
        l.EventLabel;
    """

    event_label_data = get_data_from_db(event_label_query)
    if event_label_data is None:
        st.error("Failed to fetch EventLabel data.")
    else:
        plot_bar_chart(event_label_data, f'Active Futures Principal by EventLabel ({league_name})', 'Total Dollars At Stake ($)')

        # Filter for EventLabel
        event_labels = sorted(set(row['EventLabel'] for row in event_label_data))
        event_label_option = st.selectbox('Select EventLabel', event_labels)

        if event_label_option:
            # Filter for EventType
            event_type_query = f"""
            SELECT DISTINCT l.EventType
            FROM 
                bets b
            JOIN 
                legs l ON b.WagerID = l.WagerID
            WHERE
                l.LeagueName = '{league_name}'
                AND l.EventLabel = '{event_label_option}'
                AND b.WhichBankroll = 'GreenAleph'
                AND l.IsFuture = 'Yes'
                AND b.WLCA = 'Active';
            """
            event_type_data = get_data_from_db(event_type_query)
            if event_type_data is None:
                st.error("Failed to fetch EventType data.")
            else:
                event_types = sorted(set(row['EventType'] for row in event_type_data))
                event_type_option = st.selectbox('Select EventType', event_types)

                if event_type_option:
                    # Query for combined chart (DollarsAtStake and PotentialPayout for Active Bets)
                    combined_query = f"""
                    WITH DistinctBets AS (
                        SELECT DISTINCT WagerID, DollarsAtStake, PotentialPayout
                        FROM bets
                        WHERE WhichBankroll = 'GreenAleph'
                          AND LegCount = 1
                          AND WLCA = 'Active'
                    )
                    SELECT 
                        l.ParticipantName,
                        SUM(db.DollarsAtStake) AS TotalDollarsAtStake,
                        SUM(db.PotentialPayout) AS TotalPotentialPayout
                    FROM 
                        DistinctBets db
                    JOIN 
                        legs l ON db.WagerID = l.WagerID
                    WHERE
                        l.LeagueName = '{league_name}'
                        AND l.EventLabel = '{event_label_option}'
                        AND l.EventType = '{event_type_option}'
                    GROUP BY 
                        l.ParticipantName;
                    """
                
                    combined_data = get_data_from_db(combined_query)
                    if combined_data is None:
                        st.error("Failed to fetch combined data.")
                    else:
                        df = pd.DataFrame(combined_data)
                        if not df.empty:
                            # Modify to multiply TotalDollarsAtStake by -1 to move it in the negative direction
                            df['TotalDollarsAtStake'] = -df['TotalDollarsAtStake'].astype(float).round(0)
                            df['TotalPotentialPayout'] = df['TotalPotentialPayout'].astype(float).round(0)
                
                            # Sort values by TotalDollarsAtStake in ascending order
                            df = df.sort_values('TotalDollarsAtStake', ascending=True)
                
                            # Define the colors (lightblue for DollarsAtStake, beige for PotentialPayout)
                            color_dollars_at_stake = 'lightblue'
                            color_potential_payout = 'beige'
                
                            # Create the plot
                            fig, ax = plt.subplots(figsize=(18, 12))
                
                            # Plot TotalDollarsAtStake as a negative value (below the x-axis)
                            bars1 = ax.bar(df['ParticipantName'], df['TotalDollarsAtStake'], 
                                           color=color_dollars_at_stake, width=0.4, edgecolor='black')
                
                            # Plot TotalPotentialPayout as a positive value (above the x-axis)
                            bars2 = ax.bar(df['ParticipantName'], df['TotalPotentialPayout'], 
                                           color=color_potential_payout, width=0.4, edgecolor='black')
                
                            # Add labels and title
                            ax.set_ylabel('Total Amount ($)', fontsize=20, fontweight='bold', color='black')
                            ax.set_title(f'Active Futures Principal & Potential Payout by ParticipantName for {event_type_option} - {event_label_option} ({league_name}, Straight Bets Only', fontsize=24, fontweight='bold', color='black')
                
                            # Create FontProperties object for bold tick labels
                            tick_label_font = fm.FontProperties(weight='bold', size=16)
                
                            # Increase font size and make tick labels bold; adjust the position of x-axis labels using labelpad
                            ax.tick_params(axis='x', labelsize=16, labelcolor='black', labelrotation=45, pad=10)
                            ax.tick_params(axis='y', labelsize=16, labelcolor='black')
                
                            # Apply bold font to x and y tick labels
                            for label in ax.get_xticklabels():
                                label.set_fontproperties(tick_label_font)
                            for label in ax.get_yticklabels():
                                label.set_fontproperties(tick_label_font)
                
                            # Annotate each bar for TotalDollarsAtStake (below the bar since it's negative)
                            for bar1 in bars1:
                                height = bar1.get_height()
                                ax.annotate(f'{abs(height):,.0f}', xy=(bar1.get_x() + bar1.get_width() / 2, height),
                                            xytext=(0, -15),  # Move label down
                                            textcoords="offset points",
                                            ha='center', va='bottom', fontsize=14, fontweight='bold', color='black')
                
                            # Annotate each bar for TotalPotentialPayout (above the bar)
                            for bar2 in bars2:
                                height2 = bar2.get_height()
                                ax.annotate(f'{height2:,.0f}', 
                                            xy=(bar2.get_x() + bar2.get_width() / 2, height2),
                                            xytext=(0, 3), textcoords="offset points",
                                            ha='center', va='bottom', fontsize=14, fontweight='bold', color='black')
                
                            # Add a horizontal line at y=0 for reference
                            ax.axhline(0, color='black', linewidth=0.8)
                
                            # Set the background color to white
                            ax.set_facecolor('white')
                
                            # Add a border around the plot
                            for spine in ax.spines.values():
                                spine.set_edgecolor('black')
                                spine.set_linewidth(1.2)
                
                            # Add the legend with the correct order for the bars
                            ax.legend([bars2, bars1], ['Potential Payout', 'Active Principal'], loc='upper right', fontsize=14)
                
                            # Adjust the layout
                            plt.tight_layout()
                
                            # Use Streamlit to display the chart
                            st.pyplot(fig)
                        else:
                            st.error("No data available for the selected filters.")


                
                                
                                
                
                
                
                
                
elif page == "MLB Charts":
    # MLB Charts
    st.title('MLB Active Bets - GA1')

    # SQL query to fetch data for the main bar chart
    main_query = """
    WITH DistinctBets AS (
        SELECT DISTINCT WagerID, DollarsAtStake
        FROM bets
        WHERE WhichBankroll = 'GreenAleph'
          AND WLCA = 'Active'
    ),
    EventTypeSums AS (
        SELECT 
            l.EventType,
            ROUND(SUM(db.DollarsAtStake), 0) AS TotalDollarsAtStake
        FROM 
            DistinctBets db
        JOIN 
            (SELECT DISTINCT WagerID, EventType, LeagueName FROM legs) l ON db.WagerID = l.WagerID
        WHERE
            l.LeagueName = 'MLB'
        GROUP BY 
            l.EventType
    )
    SELECT * FROM EventTypeSums

    UNION ALL

    SELECT 
        'Total' AS EventType,
        ROUND(SUM(db.DollarsAtStake), 0) AS TotalDollarsAtStake
    FROM 
        DistinctBets db
    JOIN 
        (SELECT DISTINCT WagerID, LeagueName FROM legs) l ON db.WagerID = l.WagerID
    WHERE
        l.LeagueName = 'MLB';
    """

    # Fetch the data for the main bar chart
    main_data = get_data_from_db(main_query)

    # Check if data is fetched successfully
    if main_data is None:
        st.error("Failed to fetch data from the database.")
    else:
        # Create a DataFrame from the fetched data
        main_df = pd.DataFrame(main_data)

        # Display the fetched data
       # st.subheader('Total Dollars At Stake by EventType (GA1)')
        
        # Create data for visualization
        main_df['TotalDollarsAtStake'] = main_df['TotalDollarsAtStake'].astype(float).round(0)

        # Sort the DataFrame by 'TotalDollarsAtStake' in ascending order
        main_df = main_df.sort_values('TotalDollarsAtStake', ascending=True)

        # Define pastel colors for the main chart
        pastel_colors = ['#a0d8f1', '#f4a261', '#e76f51', '#8ecae6', '#219ebc', '#023047', '#ffb703', '#fb8500', '#d4a5a5', '#9ab0a8']

        # Plot the main bar chart
        fig, ax = plt.subplots(figsize=(15, 10))
        bars = ax.bar(main_df['EventType'], main_df['TotalDollarsAtStake'], color=[pastel_colors[i % len(pastel_colors)] for i in range(len(main_df['EventType']))], width=0.6, edgecolor='black')

        # Add labels and title
        ax.set_title('Total Active Principal by EventType (GA1)', fontsize=18, fontweight='bold')
        ax.set_ylabel('Total Dollars At Stake ($)', fontsize=14, fontweight='bold')

        # Annotate each bar with the value (no dollar sign)
        for bar in bars:
            height = bar.get_height()
            ax.annotate(f'{height:,.0f}', xy=(bar.get_x() + bar.get_width() / 2, height),
                        xytext=(0, 3), textcoords="offset points",
                        ha='center', va='bottom', fontsize=14, fontweight='bold', color='black')

        # Rotate the x-axis labels to 45 degrees
        plt.xticks(rotation=45, ha='right', fontsize=14, fontweight='bold')

        # Add horizontal line at y=0 for reference
        ax.axhline(0, color='black', linewidth=0.8)

        # Set background color to white
        ax.set_facecolor('white')

        # Add border around the plot
        for spine in ax.spines.values():
            spine.set_edgecolor('black')
            spine.set_linewidth(1.2)

        # Adjust layout
        plt.tight_layout()

        # Use Streamlit to display the chart
        st.pyplot(fig)

        # Add filter for EventType, excluding "Total"
        event_type_option = st.selectbox('Select EventType', sorted(main_df[main_df['EventType'] != 'Total']['EventType'].unique()))

        if event_type_option:
            # SQL query to fetch data for the EventLabel dropdown
            event_label_query = f"""
            SELECT DISTINCT l.EventLabel
            FROM 
                bets b
            JOIN 
                legs l ON b.WagerID = l.WagerID
            WHERE
                l.LeagueName = 'MLB'
                AND l.EventType = '{event_type_option}'
                AND b.WhichBankroll = 'GreenAleph'
                AND b.WLCA = 'Active';
            """
            
            # Fetch the EventLabel data
            event_label_data = get_data_from_db(event_label_query)

            if event_label_data is None:
                st.error("Failed to fetch data from the database.")
            else:
                event_labels = [row['EventLabel'] for row in event_label_data]
                event_label_option = st.selectbox('Select EventLabel', sorted(event_labels))

                if event_label_option:
                    # SQL query to fetch data for the combined bar chart (DollarsAtStake and PotentialPayout)
                    combined_query = f"""
                    WITH DistinctBets AS (
                        SELECT DISTINCT WagerID, DollarsAtStake, PotentialPayout
                        FROM bets
                        WHERE WhichBankroll = 'GreenAleph'
                          AND WLCA = 'Active'
                          AND LegCount = 1
                    )
                    SELECT 
                        l.ParticipantName,
                        SUM(db.DollarsAtStake) AS TotalDollarsAtStake,
                        SUM(db.PotentialPayout) AS TotalPotentialPayout
                    FROM 
                        DistinctBets db
                    JOIN 
                        legs l ON db.WagerID = l.WagerID
                    WHERE
                        l.LeagueName = 'MLB'
                        AND l.EventType = '{event_type_option}'
                        AND l.EventLabel = '{event_label_option}'
                    GROUP BY 
                        l.ParticipantName;
                    """
                
                    # Fetch the combined data
                    combined_data = get_data_from_db(combined_query)
                
                    # Check if data is fetched successfully
                    if combined_data is None:
                        st.error("Failed to fetch data from the database.")
                    else:
                        # Create a DataFrame from the fetched data
                        combined_df = pd.DataFrame(combined_data)
                
                        # Modify to multiply TotalDollarsAtStake by -1 for the chart (to show negative values)
                        combined_df['TotalDollarsAtStake'] = -combined_df['TotalDollarsAtStake'].astype(float).round(0)
                        combined_df['TotalPotentialPayout'] = combined_df['TotalPotentialPayout'].astype(float).round(0)
                
                        # Sort the DataFrame by 'TotalDollarsAtStake' in ascending order
                        combined_df = combined_df.sort_values('TotalDollarsAtStake', ascending=True)
                
                        # Define colors for DollarsAtStake and PotentialPayout (same as NFL example)
                        color_dollars_at_stake = 'lightblue'  # Light blue for DollarsAtStake
                        color_potential_payout = 'beige'      # Beige for PotentialPayout
                
                        # Plot the combined bar chart
                        fig, ax = plt.subplots(figsize=(18, 12))
                
                        # Plot TotalDollarsAtStake moving downward from the x-axis
                        bars1 = ax.bar(combined_df['ParticipantName'], combined_df['TotalDollarsAtStake'], 
                                       color=color_dollars_at_stake, width=0.4, edgecolor='black')
                
                        # Plot TotalPotentialPayout moving upward from the x-axis
                        bars2 = ax.bar(combined_df['ParticipantName'], combined_df['TotalPotentialPayout'], 
                                       color=color_potential_payout, width=0.4, edgecolor='black')
                
                        # Add labels and title
                        ax.set_ylabel('USD ($)', fontsize=16, fontweight='bold')
                        ax.set_title(f'Total Active Principal & Potential Payout (Straight Bets Only)', fontsize=18, fontweight='bold')
                
                        # Annotate each bar with the TotalDollarsAtStake value below the bar
                        for bar1 in bars1:
                            height = bar1.get_height()
                            ax.annotate(f'{abs(height):,.0f}', xy=(bar1.get_x() + bar1.get_width() / 2, height),
                                        xytext=(0, -15),  # Move the labels further down below the bars
                                        textcoords="offset points",
                                        ha='center', va='bottom', fontsize=12, fontweight='bold', color='black')
                
                        # Annotate each bar with the TotalPotentialPayout value above the bar
                        for bar2 in bars2:
                            height2 = bar2.get_height()
                            ax.annotate(f'{height2:,.0f}', 
                                        xy=(bar2.get_x() + bar2.get_width() / 2, height2),
                                        xytext=(0, 3), textcoords="offset points",
                                        ha='center', va='bottom', fontsize=12, fontweight='bold', color='black')
                
                        # Rotate the x-axis labels to 45 degrees for better readability
                        plt.xticks(rotation=45, ha='right', fontsize=14, fontweight='bold')
                
                        # Add horizontal line at y=0 for reference
                        ax.axhline(0, color='black', linewidth=0.8)
                
                        # Set background color to white
                        ax.set_facecolor('white')
                
                        # Add border around the plot
                        for spine in ax.spines.values():
                            spine.set_edgecolor('black')
                            spine.set_linewidth(1.2)
                
                        # Extend y-axis range
                        ax.set_ylim(min(combined_df['TotalDollarsAtStake']) - 5000, max(combined_df['TotalPotentialPayout']) + 5000)
                
                        # Add legend
                        ax.legend([bars2, bars1], ['Potential Payout', 'Active Principal'])
                
                        # Adjust layout
                        plt.tight_layout()
                
                        # Use Streamlit to display the combined chart
                        st.pyplot(fig)
                

                   
                                                            









                        
elif page == "MLB Principal Tables":
    # MLB Principal Tables
    st.title('MLB Principal Tables - GA1')
    
    # SQL query to fetch the data for Active Straight Bets
    straight_bets_query = """
    WITH BaseQuery AS (
        SELECT l.EventType, 
               l.ParticipantName, 
               ROUND(SUM(b.DollarsAtStake)) AS TotalDollarsAtStake, 
               ROUND(SUM(b.PotentialPayout)) AS TotalPotentialPayout,
               (SUM(b.DollarsAtStake) / SUM(b.PotentialPayout)) * 100 AS ImpliedProbability
        FROM bets b    
        JOIN legs l ON b.WagerID = l.WagerID
        WHERE b.LegCount = 1
          AND l.LeagueName = 'MLB'
          AND b.WhichBankroll = 'GreenAleph'
          AND b.WLCA = 'Active'
        GROUP BY l.EventType, l.ParticipantName
        
        UNION ALL
        
        SELECT l.EventType, 
               'Total by EventType' AS ParticipantName, 
               ROUND(SUM(b.DollarsAtStake)) AS TotalDollarsAtStake, 
               NULL AS TotalPotentialPayout,
               (SUM(b.DollarsAtStake) / SUM(b.PotentialPayout)) * 100 AS ImpliedProbability
        FROM bets b
        JOIN legs l ON b.WagerID = l.WagerID
        WHERE b.LegCount = 1
          AND l.LeagueName = 'MLB'
          AND b.WhichBankroll = 'GreenAleph'
          AND b.WLCA = 'Active'
        GROUP BY l.EventType
    
        UNION ALL
    
        SELECT NULL AS EventType, 
               'Cumulative Total' AS ParticipantName, 
               ROUND(SUM(b.DollarsAtStake)) AS TotalDollarsAtStake, 
               NULL AS TotalPotentialPayout,
               (SUM(b.DollarsAtStake) / SUM(b.PotentialPayout)) * 100 AS ImpliedProbability
        FROM bets b
        JOIN legs l ON b.WagerID = l.WagerID
        WHERE b.LegCount = 1
          AND l.LeagueName = 'MLB'
          AND b.WhichBankroll = 'GreenAleph'
          AND b.WLCA = 'Active'
    )
    
    SELECT EventType, 
           ParticipantName, 
           FORMAT(TotalDollarsAtStake, 0) AS TotalDollarsAtStake, 
           FORMAT(TotalPotentialPayout, 0) AS TotalPotentialPayout,
           CONCAT(FORMAT(ImpliedProbability, 2), '%') AS ImpliedProbability
    FROM (
        SELECT *, 
               ROW_NUMBER() OVER (PARTITION BY EventType ORDER BY (ParticipantName = 'Total by EventType') ASC, ParticipantName) AS RowNum
        FROM BaseQuery
    ) AS SubQuery
    ORDER BY EventType, RowNum;
    """
    
    # SQL query to fetch the data for Active Parlay Bets
    parlay_bets_query = """
    SELECT 
        l.LegID,
        l.EventType,
        l.ParticipantName,
        b.DollarsAtStake,
        b.PotentialPayout,
        b.ImpliedOdds,
        l.EventLabel,
        l.LegDescription
    FROM 
        bets b
    JOIN 
        legs l ON b.WagerID = l.WagerID
    WHERE 
        l.LeagueName = 'MLB'
        AND b.WhichBankroll = 'GreenAleph'
        AND b.WLCA = 'Active'
        AND b.LegCount > 1;
    """
    
    # Fetch the data for Active Straight Bets
    straight_bets_data = get_data_from_db(straight_bets_query)
    
    # Fetch the data for Active Parlay Bets 
    parlay_bets_data = get_data_from_db(parlay_bets_query)
    
    # Display the data
    if straight_bets_data:
        straight_bets_df = pd.DataFrame(straight_bets_data)
        st.subheader('Active Straight Bets in GA1')
        st.table(straight_bets_df)
    
    if parlay_bets_data:
        parlay_bets_df = pd.DataFrame(parlay_bets_data)
        st.subheader('Active Parlay Bets in GA1')
        st.table(parlay_bets_df)



elif page == "NBA Participant Positions":
    # NBA Participant Positions
    st.title('NBA Participant Positions - GA1')

    # Fetch the list of participant names for the dropdown
    participants_query = """
    SELECT DISTINCT ParticipantName 
    FROM legs l
    JOIN bets b ON l.WagerID = b.WagerID
    WHERE l.LeagueName = 'NBA' AND b.WhichBankroll = 'GreenAleph'
    ORDER BY ParticipantName ASC;
    """
    participants = get_data_from_db(participants_query)

    if participants is not None:
        participant_names = [participant['ParticipantName'] for participant in participants]
        participant_selected = st.selectbox('Select Participant', participant_names)

        if participant_selected:
            wlca_filter = st.selectbox('Select WLCA', ['All', 'Win', 'Loss', 'Cashout', 'Active'])
            legcount_filter = st.selectbox('Select Bet Type', ['All', 'Straight', 'Parlay'])

            # SQL query to fetch data for the selected participant
            query = """
            SELECT 
                l.LegID,
                l.EventType,
                b.DollarsAtStake,
                b.PotentialPayout,
                b.NetProfit,
                b.ImpliedOdds,
                l.EventLabel,
                l.LegDescription,
                b.Sportsbook,
                b.DateTimePlaced,
                b.LegCount
            FROM 
                bets b
            JOIN 
                legs l ON b.WagerID = l.WagerID
            WHERE 
                l.ParticipantName = %s
                AND b.WhichBankroll = 'GreenAleph'
                AND l.LeagueName = 'NBA'
            """
            params = [participant_selected]

            if wlca_filter != 'All':
                query += " AND b.WLCA = %s"
                params.append(wlca_filter)
            
            if legcount_filter == 'Straight':
                query += " AND b.LegCount = 1"
            elif legcount_filter == 'Parlay':
                query += " AND b.LegCount > 1"

            # Fetch the data for the selected participant
            data = get_data_from_db(query, params)

            # Display the data
            if data:
                df = pd.DataFrame(data)
                st.subheader(f'Bets and Legs for {participant_selected}')
                st.table(df)
            else:
                st.warning('No data found for the selected filters.')





elif page == "NFL Participant Positions":
    # NFL Participant Positions
    st.title('NFL Participant Positions - GA1')

    # Fetch the list of participant names for the dropdown
    participants_query = """
    SELECT DISTINCT ParticipantName 
    FROM legs l
    JOIN bets b ON l.WagerID = b.WagerID
    WHERE l.LeagueName = 'NFL' AND b.WhichBankroll = 'GreenAleph'
    ORDER BY ParticipantName ASC;
    """
    participants = get_data_from_db(participants_query)

    if participants is not None:
        participant_names = [participant['ParticipantName'] for participant in participants]
        participant_selected = st.selectbox('Select Participant', participant_names)

        if participant_selected:
            wlca_filter = st.selectbox('Select WLCA', ['All', 'Win', 'Loss', 'Cashout', 'Active'])
            legcount_filter = st.selectbox('Select Bet Type', ['All', 'Straight', 'Parlay'])

            # SQL query to fetch data for the selected participant
            query = """
            SELECT 
                l.LegID,
                l.EventType,
                b.DollarsAtStake,
                b.PotentialPayout,
                b.NetProfit,
                b.ImpliedOdds,
                l.EventLabel,
                l.LegDescription,
                b.Sportsbook,
                b.DateTimePlaced,
                b.LegCount
            FROM 
                bets b
            JOIN 
                legs l ON b.WagerID = l.WagerID
            WHERE 
                l.ParticipantName = %s
                AND b.WhichBankroll = 'GreenAleph'
                AND l.LeagueName = 'NFL'
            """
            params = [participant_selected]

            if wlca_filter != 'All':
                query += " AND b.WLCA = %s"
                params.append(wlca_filter)
            
            if legcount_filter == 'Straight':
                query += " AND b.LegCount = 1"
            elif legcount_filter == 'Parlay':
                query += " AND b.LegCount > 1"

            # Fetch the data for the selected participant
            data = get_data_from_db(query, params)

            # Display the data
            if data:
                df = pd.DataFrame(data)
                st.subheader(f'Bets and Legs for {participant_selected}')
                st.table(df)
            else:
                st.warning('No data found for the selected filters.')




    
    







