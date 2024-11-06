import streamlit as st
import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm

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
page = st.sidebar.radio("Go to", ["Main Page", "Principal Volume", "Betting Frequency", "NBA Charts", "NFL Charts", "Tennis Charts", "MLB Charts", "MLB Principal Tables", "MLB Participant Positions"])


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
    
            # Plot the Cumulative Realized Profit by Month line graph
            st.subheader("Cumulative Realized Profit by Month for 'GreenAleph'")
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
            ax.set_title('Cumulative Realized Profit by Month (GreenAleph)', fontsize=20, fontweight='bold')
            ax.axhline(0, color='black', linewidth=1)  # Add horizontal line at y=0
            ax.grid(True, linestyle='--', alpha=0.7)  # Add a light grid
    
            # Set x-axis labels with rotation and larger font size
            plt.xticks(rotation=45, ha='right', fontsize=12)
            plt.yticks(fontsize=12)
    
            # Add value labels above each data point, larger for visibility
            for month, profit in zip(months, cumulative_profits):
                ax.annotate(f"${profit:,.0f}", xy=(month, profit),
                            xytext=(0, 8), textcoords="offset points",
                            ha='center', fontsize=12, fontweight='bold',
                            color='green' if profit >= 0 else 'red')
    
            plt.tight_layout()
            st.pyplot(fig)
        else:
            st.warning("No data available for monthly cumulative realized profit.")







# Adding the new "Principal Volume" page
if page == "Principal Volume":
    st.title("Principal Volume (GA1)")

    # SQL query to get the total principal (dollars at stake) by month for 'GreenAleph' without including cashouts
    principal_volume_query = """
        SELECT 
            DATE_FORMAT(DateTimePlaced, '%Y-%m') AS Month,
            SUM(DollarsAtStake) AS TotalDollarsAtStake
        FROM bets
        WHERE WhichBankroll = 'GreenAleph' AND WLCA != 'Cashout'
        GROUP BY Month
        ORDER BY Month;
    """

    # SQL query to get the total principal volume by LeagueName for 'GreenAleph', summing each WagerID only once
    league_principal_volume_query = """
        SELECT 
            l.LeagueName,
            SUM(b.DollarsAtStake) AS TotalDollarsAtStake
        FROM bets b
        JOIN legs l ON b.WagerID = l.WagerID
        WHERE b.WhichBankroll = 'GreenAleph' AND b.WLCA != 'Cashout'
        AND b.WagerID IN (
            SELECT DISTINCT WagerID
            FROM bets
            WHERE LegCount <= 1 OR (LegCount > 1 AND WLCA != 'Cashout')
        )
        GROUP BY l.LeagueName
        ORDER BY TotalDollarsAtStake DESC;
    """

    # Get data from the database for the first chart
    principal_volume_data = get_data_from_db(principal_volume_query)

    if principal_volume_data:
        # Convert the data to a DataFrame for plotting
        df_principal_volume = pd.DataFrame(principal_volume_data)

        # Check if the DataFrame is not empty
        if not df_principal_volume.empty:
            df_principal_volume['Month'] = pd.to_datetime(df_principal_volume['Month'])
            df_principal_volume.set_index('Month', inplace=True)
            df_principal_volume.sort_index(inplace=True)

            # Calculate the total principal volume
            total_principal = df_principal_volume['TotalDollarsAtStake'].sum()
            total_row = pd.DataFrame({'TotalDollarsAtStake': [total_principal]}, index=['Total'])
            df_principal_volume = pd.concat([df_principal_volume, total_row])

            # Prepare x-axis labels
            x_labels = [date.strftime('%Y-%m') if isinstance(date, pd.Timestamp) else date for date in df_principal_volume.index]

            # Plot the first bar chart
            st.subheader("Total Principal Volume by Month for 'GreenAleph'")
            plt.figure(figsize=(12, 6))
            bars = plt.bar(x_labels, df_principal_volume['TotalDollarsAtStake'])
            plt.ylabel('Total Principal ($)')
            plt.title('Total Principal Volume by Month (GreenAleph)')
            plt.xticks(rotation=45, ha='right')

            # Add value labels above each bar, rounded to whole numbers
            for bar in bars:
                yval = bar.get_height()
                plt.text(bar.get_x() + bar.get_width()/2, yval, f"${yval:,.0f}", ha='center', va='bottom')

            st.pyplot(plt)
        else:
            st.warning("No data available for 'GreenAleph' principal volume.")
    else:
        st.error("Failed to retrieve data from the database.")

    # Get data from the database for the second chart
    league_principal_volume_data = get_data_from_db(league_principal_volume_query)

    if league_principal_volume_data:
        # Convert the data to a DataFrame for plotting
        df_league_principal_volume = pd.DataFrame(league_principal_volume_data)

        # Ensure correct data types for plotting
        df_league_principal_volume['LeagueName'] = df_league_principal_volume['LeagueName'].astype(str)
        df_league_principal_volume['TotalDollarsAtStake'] = df_league_principal_volume['TotalDollarsAtStake'].astype(float)

        # Sort by TotalDollarsAtStake in ascending order
        df_league_principal_volume = df_league_principal_volume.sort_values(by='TotalDollarsAtStake', ascending=True)

        # Check if the DataFrame is not empty
        if not df_league_principal_volume.empty:
            # Plot the second bar chart
            st.subheader("Principal Volume by League")
            plt.figure(figsize=(12, 6))
            plt.bar(df_league_principal_volume['LeagueName'], df_league_principal_volume['TotalDollarsAtStake'])
            plt.ylabel('Total Principal ($)')
            plt.title('Total Principal Volume by LeagueName (GreenAleph)')
            plt.xticks(rotation=45, ha='right')

            # Add value labels above each bar, rounded to whole numbers
            for index, value in enumerate(df_league_principal_volume['TotalDollarsAtStake']):
                plt.text(index, value, f"${value:,.0f}", ha='center', va='bottom')

            st.pyplot(plt)
        else:
            st.warning("No data available for 'GreenAleph' principal volume by league.")
    else:
        st.error("Failed to retrieve data from the database.")





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
            st.subheader("Number of Bets Placed by Month for 'GreenAleph'")
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
            l.LeagueName = 'NBA'
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
        l.LeagueName = 'NBA';
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

        # Use Streamlit to display the first chart
        st.pyplot(fig)

        # Add filter for EventType, sorted in alphabetical order
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
                l.LeagueName = 'NBA'
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
                        l.LeagueName = 'NBA'
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
                        ax.set_ylabel('USD ($)', fontsize=16, fontweight='bold')
                        ax.set_title(f'Active Principal & Potential Payout by ParticipantName for {event_type_option} - {event_label_option} (GA1, Straight Bets Only)', fontsize=18, fontweight='bold')
                
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
                
                        # Rotate the x-axis labels to 45 degrees
                        plt.xticks(rotation=45, ha='right', fontsize=14, fontweight='bold')
                
                        # Add legend
                        ax.legend([bars2, bars1],['Potential Payout', 'Active Principal'])
                
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
                
                        # Adjust layout
                        plt.tight_layout()
                
                        # Use Streamlit to display the combined chart
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

        # Use Streamlit to display the first chart
        st.pyplot(fig)

        # Add filter for EventType
        event_type_option = st.selectbox('Select EventType', sorted(first_chart_df[first_chart_df['EventType'] != 'Total']['EventType'].unique()))

        if event_type_option:
            # SQL query to fetch data for the EventLabel dropdown
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
                
                        # Modify to multiply TotalDollarsAtStake by -1 for the chart (to show negative values)
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
                        ax.set_ylabel('USD ($)', fontsize=16, fontweight='bold')
                        ax.set_title(f'Active Principal & Potential Payout by ParticipantName for {event_type_option} - {event_label_option} (GA1, Straight Bets Only)', fontsize=18, fontweight='bold')
                
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
                
                        # Rotate the x-axis labels to 45 degrees
                        plt.xticks(rotation=45, ha='right', fontsize=14, fontweight='bold')
                
                        # Add legend
                        ax.legend([bars2, bars1],['Potential Payout', 'Active Principal'])
                
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
                
                        # Adjust layout
                        plt.tight_layout()
                
                        # Use Streamlit to display the combined chart
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
          AND WLCA != 'Cashout'
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
        plot_bar_chart(event_label_data, f'Total Futures Principal by EventLabel ({league_name}), Excluding Cashouts', 'Total Dollars At Stake ($)')

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
                AND l.IsFuture = 'Yes';
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
                            ax.set_title(f'Total Futures Principal & Potential Payout by ParticipantName for {event_type_option} - {event_label_option} ({league_name}, Straight Bets Only, Excluding Cashouts)', fontsize=24, fontweight='bold', color='black')
                
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
                        ax.set_title(f'Total Active Principal & Potential Payout by ParticipantName for {event_type_option} - {event_label_option} (GA1, Straight Bets Only)', fontsize=18, fontweight='bold')
                
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


elif page == "MLB Participant Positions":
    # MLB Participant Positions
    st.title('MLB Participant Positions - GA1')

    # Fetch the list of participant names for the dropdown
    participants_query = """
    SELECT DISTINCT ParticipantName 
    FROM legs 
    WHERE LeagueName = 'MLB'
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
                AND l.LeagueName = 'MLB'
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



    
    
