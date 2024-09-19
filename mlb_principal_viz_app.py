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
page = st.sidebar.radio("Go to", ["GreenAleph Active Principal", "NFL Charts", "Tennis Charts", "MLB Charts", "MLB Principal Tables", "MLB Participant Positions", "Profit"])


if page == "GreenAleph Active Principal":
    # GreenAleph Active Principal
    st.title('Principal Dashboard - GreenAleph I')

    # Display last update time in the corner
    st.markdown(f"**Last Update:** {last_update_time}", unsafe_allow_html=True)

    # SQL query to fetch data
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

    # Fetch the data
    data = get_data_from_db(data_query)

    # Query to calculate total dollars deployed
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

    # Fetch the total dollars deployed
    deployed_data = get_data_from_db(deployed_query)

    # Check if data is fetched successfully
    if data is None or deployed_data is None:
        st.error("Failed to fetch data from the database.")
    else:
        # Create a DataFrame from the fetched data
        df = pd.DataFrame(data)

        # Convert TotalDollarsAtStake to float for plotting
        df['TotalDollarsAtStake'] = df['TotalDollarsAtStake'].astype(float)

        # Sort the DataFrame by TotalDollarsAtStake in ascending order
        df = df.sort_values(by='TotalDollarsAtStake')

        # Define colors for bars
        colors = ['#77dd77', '#89cff0', '#fdfd96', '#ffb347', '#aec6cf', '#cfcfc4', '#ffb6c1', '#b39eb5']
        total_color = 'lightblue'  # Light blue for the Total bar

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

        # Handle the total dollars deployed data
        if deployed_data and len(deployed_data) > 0 and 'TotalDollarsDeployed' in deployed_data[0]:
            total_dollars_deployed = deployed_data[0]['TotalDollarsDeployed']
            
            # Ensure total_dollars_deployed is a float and round it to the nearest dollar
            total_dollars_deployed = round(float(total_dollars_deployed)) if total_dollars_deployed is not None else 0
            
            # Set the goal amount
            goal_amount = 500000
            
            # Calculate the progress percentage relative to the $500k goal
            progress_percentage = min(total_dollars_deployed / goal_amount, 1)  # Ensure it does not exceed 100%
            
            # Calculate the position of the label as a percentage of the bar's width
            label_position_percentage = progress_percentage * 50  # Center the label within the light green area
        
            # Display the smaller heading
            st.markdown(f"<h4 style='text-align: center; font-weight: bold; color: black;'>Total $ Deployed (Total Active Principal - Realized Profit)</h4>", unsafe_allow_html=True)
            
            # Display the progress bar with shaded sides
            st.markdown(f"""
            <div style='width: 80%; margin: 0 auto;'>
                <div style='background-color: lightgray; height: 40px; position: relative; border-radius: 5px;'>
                    <div style='background: linear-gradient(to right, lightblue {progress_percentage * 100}%, lightgray 0%); width: 100%; height: 100%; border-radius: 5px; position: relative;'>
                        <span style='position: absolute; left: {label_position_percentage}%; top: 50%; transform: translate(-50%, -50%); color: white; font-weight: bold;'>${total_dollars_deployed:,}</span>
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)
        
            # Display the subheading directly below the progress bar
            st.markdown(f"<h5 style='text-align: center; font-weight: bold; color: gray;'>$500k Initial Deployment Goal</h5>", unsafe_allow_html=True)
        else:
            st.error("No data available for Total Dollars Deployed.")






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
                        ax.legend(['Active Principal', 'Potential Payout'])
                
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
                            df['TotalDollarsAtStake'] = df['TotalDollarsAtStake'].astype(float).round(0)
                            df['TotalPotentialPayout'] = df['TotalPotentialPayout'].astype(float).round(0)
                            df = df.sort_values('TotalDollarsAtStake', ascending=True)

                            color_dollars_at_stake = 'lightblue'
                            color_potential_payout = 'beige'

                            fig, ax = plt.subplots(figsize=(18, 12))
                            bars1 = ax.bar(df['ParticipantName'], df['TotalDollarsAtStake'], color=color_dollars_at_stake, width=0.4, edgecolor='black', label='Total Dollars At Stake')
                            bars2 = ax.bar(df['ParticipantName'], df['TotalPotentialPayout'], color=color_potential_payout, width=0.4, edgecolor='black', label='Total Potential Payout', alpha=0.6, bottom=df['TotalDollarsAtStake'])

                            ax.set_ylabel('Total Amount ($)', fontsize=16, fontweight='bold')
                            ax.set_title(f'Total Futures Principal Overlaid on Potential Payout by ParticipantName for {event_type_option} - {event_label_option} ({league_name}, Straight Bets Only, Excluding Cashouts)', fontsize=18, fontweight='bold')

                            for bar1 in bars1:
                                height = bar1.get_height()
                                ax.annotate(f'{height:,.0f}', xy=(bar1.get_x() + bar1.get_width() / 2, height),
                                            xytext=(0, 3), textcoords="offset points",
                                            ha='center', va='bottom', fontsize=12, fontweight='bold', color='black')

                            for bar1, bar2 in zip(bars1, bars2):
                                height1 = bar1.get_height()
                                height2 = bar2.get_height()
                                total_height = height1 + height2
                                ax.annotate(f'{height2:,.0f}', 
                                            xy=(bar2.get_x() + bar2.get_width() / 2, total_height),
                                            xytext=(0, 3), textcoords="offset points",
                                            ha='center', va='bottom', fontsize=12, fontweight='bold', color='black')

                            plt.xticks(rotation=45, ha='right', fontsize=14, fontweight='bold')
                            ax.axhline(0, color='black', linewidth=0.8)
                            ax.set_facecolor('white')
                            for spine in ax.spines.values():
                                spine.set_edgecolor('black')
                                spine.set_linewidth(1.2)
                            ax.legend()
                            plt.tight_layout()
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
                        ax.legend([bars1, bars2]['Active Principal', 'Potential Payout'])
                
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



elif page == "Profit":
    # Profit Page
    import streamlit as st

    # Main title
    st.title('Realized Profit - GA1')

    # Subtitle
    st.subheader('"Total" incorporates all bets, but straight bets ONLY by League Name')


    # SQL query for the new bar chart (Profit by League)
    league_profit_query = """
    WITH DistinctBets AS (
    SELECT DISTINCT b.WagerID, b.NetProfit, l.LeagueName
    FROM bets b
    JOIN legs l ON b.WagerID = l.WagerID
    WHERE b.WhichBankroll = 'GreenAleph'
    AND b.LegCount = 1),
    LeagueSums AS (
    SELECT 
        l.LeagueName,
        ROUND(SUM(db.NetProfit), 0) AS NetProfit
    FROM 
        DistinctBets db
    JOIN 
        (SELECT DISTINCT WagerID, LeagueName FROM legs) l ON db.WagerID = l.WagerID
    GROUP BY 
        l.LeagueName)
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

    # Fetch the data for the new bar chart
    league_profit_data = get_data_from_db(league_profit_query)
    if league_profit_data is None:
        st.error("Failed to fetch league profit data from the database.")
    else:
        # Create a DataFrame from the fetched data
        league_profit_df = pd.DataFrame(league_profit_data)
        
        # Create the bar chart
        fig, ax = plt.subplots(figsize=(15, 8))
        bar_colors = league_profit_df['NetProfit'].apply(lambda x: 'green' if x > 0 else 'red')  # Green for positive, Red for negative
        bars = ax.bar(league_profit_df['LeagueName'], league_profit_df['NetProfit'], color=bar_colors, edgecolor='black')

        # Adding titles and labels
        ax.set_title('Realized Profit by League', fontsize=18, fontweight='bold')
        ax.set_xlabel('League Name', fontsize=16, fontweight='bold')
        ax.set_ylabel('Realized Profit ($)', fontsize=16, fontweight='bold')

        # Annotate each bar with the value
        for bar in bars:
            height = bar.get_height()
            ax.annotate(f'${height:,.0f}', xy=(bar.get_x() + bar.get_width() / 2, height),
                        xytext=(0, 3), textcoords="offset points",
                        ha='center', va='bottom', fontsize=12, fontweight='bold', color='black')

        # Rotate the x-axis labels to 45 degrees
        plt.xticks(rotation=45, ha='right', fontsize=14, fontweight='bold')

        # Set background color to white
        ax.set_facecolor('white')
        plt.gcf().set_facecolor('white')

        # Add border around the plot
        for spine in ax.spines.values():
            spine.set_edgecolor('black')
            spine.set_linewidth(1.2)

        # Adjust y-axis range
        ymin = league_profit_df['NetProfit'].min() - 500
        ymax = league_profit_df['NetProfit'].max() + 1500
        ax.set_ylim(ymin, ymax)

        # Adjust layout
        plt.tight_layout()

        # Use Streamlit to display the chart
        st.pyplot(fig)

   

