import streamlit as st
import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt

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
page = st.sidebar.radio("Go to", ["GreenAleph Active Principal", "MLB Charts", "MLB Principal Tables", "MLB Participant Positions", "Profit"])

if page == "GreenAleph Active Principal":
    # GreenAleph Active Principal
    st.title('Principal Dashboard - GreenAleph I')

    # Display last update time in the corner
    st.markdown(f"**Last Update:** {last_update_time}", unsafe_allow_html=True)

    # SQL query to fetch data
    data_query = """
    WITH DistinctBets AS (
        SELECT DISTINCT WagerID, DollarsAtStake
        FROM bets
        WHERE WhichFund = 'GreenAleph'
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

    # Fetch the data
    data = get_data_from_db(data_query)

    # Check if data is fetched successfully
    if data is None:
        st.error("Failed to fetch data from the database.")
    else:
        # Create a DataFrame from the fetched data
        df = pd.DataFrame(data)

        # Convert TotalDollarsAtStake to float for plotting
        df['TotalDollarsAtStake'] = df['TotalDollarsAtStake'].astype(float)

        # Sort the DataFrame by TotalDollarsAtStake in ascending order
        df = df.sort_values(by='TotalDollarsAtStake')

        # Display the fetched data
        st.subheader(f'Total Dollars At Stake for GA1 (Active)')

        # Define colors for bars
        colors = ['#77dd77', '#89cff0', '#fdfd96', '#ffb347', '#aec6cf', '#cfcfc4', '#ffb6c1', '#b39eb5']
        total_color = '#006400'  # Dark green for the Total bar

        # Create color list ensuring 'Total' bar is dark green
        bar_colors = [total_color if name == 'Total' else colors[i % len(colors)] for i, name in enumerate(df['LeagueName'])]

        # Plot the bar chart
        fig, ax = plt.subplots(figsize=(15, 10))
        bars = ax.bar(df['LeagueName'], df['TotalDollarsAtStake'], color=bar_colors, width=0.6, edgecolor='black')

        # Add labels and title
        ax.set_title('GA1: Total Active Principal', fontsize=18, fontweight='bold')
        ax.set_ylabel('Total Dollars At Stake ($)', fontsize=14, fontweight='bold')

        # Annotate each bar with the value
        for bar in bars:
            height = bar.get_height()
            ax.annotate(f'${height:,.0f}', xy=(bar.get_x() + bar.get_width() / 2, height),
                        xytext=(0, 3), textcoords="offset points",
                        ha='center', va='bottom', fontsize=12, fontweight='bold', color='black')

        # Rotate the x-axis labels to 45 degrees
        plt.xticks(rotation=45, ha='right')

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

elif page == "MLB Charts":
    # MLB Charts
    st.title('MLB Active Bets - GA1')

    # SQL query to fetch data for the main bar chart
    main_query = """
    WITH DistinctBets AS (
        SELECT DISTINCT WagerID, DollarsAtStake
        FROM bets
        WHERE WhichFund = 'GreenAleph'
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
        st.subheader('Total Dollars At Stake by EventType (GA1)')
        
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
        ax.set_title('GA1: Total Active Principal', fontsize=18, fontweight='bold')
        ax.set_ylabel('Total Dollars At Stake ($)', fontsize=14, fontweight='bold')

        # Annotate each bar with the value (no dollar sign)
        for bar in bars:
            height = bar.get_height()
            ax.annotate(f'{height:,.0f}', xy=(bar.get_x() + bar.get_width() / 2, height),
                        xytext=(0, 3), textcoords="offset points",
                        ha='center', va='bottom', fontsize=14, fontweight='bold', color='black')

        # Rotate the x-axis labels to 45 degrees
        plt.xticks(rotation=45, ha='right')

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
                AND b.WhichFund = 'GreenAleph'
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
                        WHERE WhichFund = 'GreenAleph'
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
                        
                        # Display the fetched data
                        st.subheader(f'Total Dollars At Stake and Potential Payout by ParticipantName for {event_type_option} - {event_label_option} (Straight Bets Only)')
                        
                        # Create data for visualization
                        combined_df['TotalDollarsAtStake'] = combined_df['TotalDollarsAtStake'].astype(float).round(0)
                        combined_df['TotalPotentialPayout'] = combined_df['TotalPotentialPayout'].astype(float).round(0)
                        
                        # Sort the DataFrame by 'TotalDollarsAtStake' in ascending order
                        combined_df = combined_df.sort_values('TotalDollarsAtStake', ascending=True)
                        
                        # Define colors for DollarsAtStake and PotentialPayout
                        color_dollars_at_stake = '#219ebc'  # Blue color
                        color_potential_payout = '#f4a261'  # Light orange
                        
                        # Plot the combined bar chart
                        fig, ax = plt.subplots(figsize=(18, 12))
                        bars1 = ax.bar(combined_df['ParticipantName'], combined_df['TotalPotentialPayout'], color=color_potential_payout, width=0.4, edgecolor='black', label='Total Potential Payout', alpha=0.6, bottom=combined_df['TotalDollarsAtStake'])
                        bars2 = ax.bar(combined_df['ParticipantName'], combined_df['TotalDollarsAtStake'], color=color_dollars_at_stake, width=0.4, edgecolor='black', label='Total Dollars At Stake')
                        
                        # Add labels and title
                        ax.set_ylabel('Total Amount ($)', fontsize=16, fontweight='bold')  # Enlarge y-axis label
                        ax.set_title(f'Total Active Principal and Potential Payout by ParticipantName for {event_type_option} - {event_label_option} (GA1, Straight Bets Only)', fontsize=18, fontweight='bold')
                        
                        # Annotate each bar with the value (no dollar sign)
                        for bar1, bar2 in zip(bars1, bars2):
                            height1 = bar1.get_height() + bar2.get_height()
                            height2 = bar2.get_height()
                            ax.annotate(f'{height1:,.0f}', xy=(bar1.get_x() + bar1.get_width() / 2, height1),
                                        xytext=(0, 3), textcoords="offset points",
                                        ha='center', va='bottom', fontsize=12, fontweight='bold', color='black')
                            ax.annotate(f'{height2:,.0f}', xy=(bar2.get_x() + bar2.get_width() / 2, height2),
                                        xytext=(0, 3), textcoords="offset points",
                                        ha='center', va='bottom', fontsize=12, fontweight='bold', color='black')
                        
                        # Rotate the x-axis labels to 45 degrees and enlarge them
                        plt.xticks(rotation=45, ha='right', fontsize=14, fontweight='bold')
                        
                        # Add horizontal line at y=0 for reference
                        ax.axhline(0, color='black', linewidth=0.8)
                        
                        # Set background color to white
                        ax.set_facecolor('white')
                        
                        # Add border around the plot
                        for spine in ax.spines.values():
                            spine.set_edgecolor('black')
                            spine.set_linewidth(1.2)
                        
                        # Add legend
                        ax.legend()
                        
                        # Adjust layout
                        plt.tight_layout()
                        
                        # Use Streamlit to display the chart
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
          AND b.WhichFund = 'GreenAleph'
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
          AND b.WhichFund = 'GreenAleph'
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
          AND b.WhichFund = 'GreenAleph'
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
        AND b.WhichFund = 'GreenAleph'
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
                AND b.WhichFund = 'GreenAleph'
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

elif page == "Profit":
    # Profit Page
    st.title('Realized Profit Over Time - GA1')

    # Fetch the distinct LeagueName for the dropdown
    event_type_query = "SELECT DISTINCT LeagueName FROM legs ORDER BY LeagueName ASC;"
    event_types = get_data_from_db(event_type_query)

    if event_types is None:
        st.error("Failed to fetch event types from the database.")
    else:
        event_type_names = [event['LeagueName'] for event in event_types]
        event_type_names.insert(0, "All")  # Add "All" to the beginning of the list

        # Dropdown menu for selecting LeagueName
        selected_event_type = st.selectbox('Select LeagueName', event_type_names)

        # SQL query to fetch data, filter by LeagueName if not "All"
        if selected_event_type == "All":
            profit_query = """
            SELECT DateTimePlaced, NetProfit 
            FROM bets 
            WHERE WhichFund = 'GreenAleph'
            """
        else:
            profit_query = """
            SELECT DateTimePlaced, NetProfit 
            FROM bets 
            JOIN legs ON bets.WagerID = legs.WagerID
            WHERE WhichFund = 'GreenAleph' 
              AND legs.LeagueName = %s
            """
            params = [selected_event_type]

        # Fetch the data
        data = get_data_from_db(profit_query, params if selected_event_type != "All" else None)

        if data is None:
            st.error("Failed to fetch data from the database.")
        else:
            df = pd.DataFrame(data)
            if 'DateTimePlaced' not in df.columns:
                st.error("The 'DateTimePlaced' column is missing from the data.")
            else:
                try:
                    # Ensure DateTimePlaced is a datetime object
                    df['DateTimePlaced'] = pd.to_datetime(df['DateTimePlaced'])

                    # Filter data to start from March 2024
                    df = df[df['DateTimePlaced'] >= '2024-03-01']

                    # Sort by DateTimePlaced
                    df.sort_values(by='DateTimePlaced', inplace=True)

                    # Resample to monthly periods
                    df.set_index('DateTimePlaced', inplace=True)
                    df = df.resample('M').sum().reset_index()

                    # Calculate the cumulative net profit
                    df['Cumulative Net Profit'] = df['NetProfit'].cumsum()

                    # Create the bar graph
                    fig, ax = plt.subplots(figsize=(15, 10))

                    # Color bars based on positive or negative values
                    bar_colors = df['Cumulative Net Profit'].apply(lambda x: 'gray' if x < 0 else 'green')

                    bars = ax.bar(df['DateTimePlaced'].dt.strftime('%Y-%m'), df['Cumulative Net Profit'], color=bar_colors, width=0.6, edgecolor='black')

                    # Adding titles and labels
                    ax.set_title('Cumulative Realized Profit Over Time', fontsize=18, fontweight='bold')
                    ax.set_xlabel('Month of Bet Placed', fontsize=14, fontweight='bold')
                    ax.set_ylabel('USD ($)', fontsize=14, fontweight='bold')

                    # Annotate each bar with the value, excluding the zero value labels
                    for bar in bars:
                        height = bar.get_height()
                        if height != 0:
                            ax.annotate(f'${height:,.0f}', xy=(bar.get_x() + bar.get_width() / 2, height),
                                        xytext=(0, 3 if height >= 0 else -3), textcoords="offset points",
                                        ha='center', va='bottom' if height >= 0 else 'top', fontsize=12, fontweight='bold', color='black')

                    # Rotate the x-axis labels to 45 degrees
                    plt.xticks(rotation=45, ha='right')

                    # Add horizontal line at y=0 for reference
                    ax.axhline(0, color='black', linewidth=1.5)

                    # Set background color to white
                    ax.set_facecolor('white')
                    plt.gcf().set_facecolor('white')

                    # Add border around the plot
                    for spine in ax.spines.values():
                        spine.set_edgecolor('black')
                        spine.set_linewidth(1.2)

                    # Set y-axis limit to include positive territory and go a few hundred dollars below the lowest bar
                    ymin = df['Cumulative Net Profit'].min() - 500
                    ymax = df['Cumulative Net Profit'].max() + 500
                    ax.set_ylim(ymin, ymax + 500)

                    # Adjust layout
                    plt.tight_layout()

                    # Use Streamlit to display the chart
                    st.pyplot(fig)
                except Exception as e:
                    st.error(f"Error processing data: {e}")
