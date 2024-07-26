# import streamlit as st
# import mysql.connector
# import pandas as pd
# import matplotlib.pyplot as plt

# # Retrieve secrets from Streamlit
# db_host = st.secrets["DB_HOST"]
# db_user = st.secrets["DB_USER"]
# db_password = st.secrets["DB_PASSWORD"]
# db_name = st.secrets["DB_NAME"]

# # Function to get data from MySQL database
# def get_data_from_db(query):
#     try:
#         conn = mysql.connector.connect(
#             host=db_host,
#             user=db_user,
#             password=db_password,
#             database=db_name
#         )
#         cursor = conn.cursor(dictionary=True)
#         cursor.execute(query)
#         data = cursor.fetchall()
#         cursor.close()
#         conn.close()
#         return data
#     except mysql.connector.Error as err:
#         st.error(f"Error: {err}")
#         return None

# # Streamlit App
# st.title('MLB Active Principal in GreenAleph Fund')

# # SQL query to fetch data for the main bar chart
# main_query = """
# WITH DistinctBets AS (
#     SELECT DISTINCT WagerID, DollarsAtStake
#     FROM bets
#     WHERE WhichFund = 'GreenAleph'
#       AND WLCA = 'Active'
# )
# SELECT 
#     l.EventType,
#     SUM(b.DollarsAtStake) AS TotalDollarsAtStake
# FROM 
#     DistinctBets b
# JOIN 
#     legs l ON b.WagerID = l.WagerID
# WHERE
#     l.LeagueName = 'MLB'
# GROUP BY 
#     l.EventType

# UNION ALL

# SELECT 
#     'Total' AS EventType,
#     SUM(b.DollarsAtStake) AS TotalDollarsAtStake
# FROM 
#     DistinctBets b
# JOIN 
#     legs l ON b.WagerID = l.WagerID
# WHERE
#     l.LeagueName = 'MLB';
# """

# # Fetch the data for the main bar chart
# main_data = get_data_from_db(main_query)

# # Check if data is fetched successfully
# if main_data is None:
#     st.error("Failed to fetch data from the database.")
# else:
#     # Create a DataFrame from the fetched data
#     main_df = pd.DataFrame(main_data)

#     # Display the fetched data
#     st.subheader('Total Dollars At Stake by EventType (GreenAleph Fund)')
    
#     # Create data for visualization
#     main_df['TotalDollarsAtStake'] = main_df['TotalDollarsAtStake'].astype(float).round(0)

#     # Sort the DataFrame by 'TotalDollarsAtStake' in ascending order
#     main_df = main_df.sort_values('TotalDollarsAtStake', ascending=True)

#     # Define pastel colors
#     pastel_colors = ['#a0d8f1', '#f4a261', '#e76f51', '#8ecae6', '#219ebc', '#023047', '#ffb703', '#fb8500', '#d4a5a5', '#9ab0a8']

#     # Plot the main bar chart
#     fig, ax = plt.subplots(figsize=(12, 8))
#     bars = ax.bar(main_df['EventType'], main_df['TotalDollarsAtStake'], color=[pastel_colors[i % len(pastel_colors)] for i in range(len(main_df['EventType']))], width=0.6, edgecolor='black')

#     # Add labels and title
#     ax.set_title('GreenAleph Fund: Total Active Principal', fontsize=18, fontweight='bold')
#     ax.set_ylabel('Total Dollars At Stake ($)', fontsize=14, fontweight='bold')

#     # Annotate each bar with the value
#     for bar in bars:
#         height = bar.get_height()
#         ax.annotate(f'${height:,.0f}', xy=(bar.get_x() + bar.get_width() / 2, height),
#                     xytext=(0, 3), textcoords="offset points",
#                     ha='center', va='bottom', fontsize=12, fontweight='bold', color='black')

#     # Rotate the x-axis labels to 45 degrees
#     plt.xticks(rotation=45, ha='right')

#     # Add horizontal line at y=0 for reference
#     ax.axhline(0, color='black', linewidth=0.8)

#     # Set background color to white
#     ax.set_facecolor('white')

#     # Add border around the plot
#     for spine in ax.spines.values():
#         spine.set_edgecolor('black')
#         spine.set_linewidth(1.2)

#     # Adjust layout
#     plt.tight_layout()

#     # Use Streamlit to display the chart
#     st.pyplot(fig)

#     # Add filter for EventType, excluding "Total"
#     event_type_option = st.selectbox('Select EventType', main_df[main_df['EventType'] != 'Total']['EventType'].unique())

#     # SQL query to fetch data for the filtered bar chart
#     filtered_query = f"""
#     WITH DistinctBets AS (
#         SELECT DISTINCT WagerID, DollarsAtStake
#         FROM bets
#         WHERE WhichFund = 'GreenAleph'
#           AND WLCA = 'Active'
#     )
#     SELECT 
#         l.ParticipantName,
#         SUM(b.DollarsAtStake) AS TotalDollarsAtStake
#     FROM 
#         DistinctBets b
#     JOIN 
#         legs l ON b.WagerID = l.WagerID
#     WHERE
#         l.LeagueName = 'MLB'
#         AND l.EventType = '{event_type_option}'
#     GROUP BY 
#         l.ParticipantName;
#     """

#     # Fetch the filtered data
#     filtered_data = get_data_from_db(filtered_query)

#     # Check if data is fetched successfully
#     if filtered_data is None:
#         st.error("Failed to fetch data from the database.")
#     else:
#         # Create a DataFrame from the fetched data
#         filtered_df = pd.DataFrame(filtered_data)

#         # Display the fetched data
#         st.subheader(f'Total Dollars At Stake by ParticipantName for {event_type_option} (GreenAleph Fund)')

#         # Create data for visualization
#         filtered_df['TotalDollarsAtStake'] = filtered_df['TotalDollarsAtStake'].astype(float).round(0)

#         # Sort the DataFrame by 'TotalDollarsAtStake' in ascending order
#         filtered_df = filtered_df.sort_values('TotalDollarsAtStake', ascending=True)

#         # Plot the filtered bar chart
#         fig, ax = plt.subplots(figsize=(12, 8))
#         bars = ax.bar(filtered_df['ParticipantName'], filtered_df['TotalDollarsAtStake'], color=[pastel_colors[i % len(pastel_colors)] for i in range(len(filtered_df['ParticipantName']))], width=0.6, edgecolor='black')

#         # Add labels and title
#         ax.set_title(f'Total Active Principal by ParticipantName for {event_type_option}', fontsize=18, fontweight='bold')
#         ax.set_ylabel('Total Dollars At Stake ($)', fontsize=14, fontweight='bold')

#         # Annotate each bar with the value
#         for bar in bars:
#             height = bar.get_height()
#             ax.annotate(f'${height:,.0f}', xy=(bar.get_x() + bar.get_width() / 2, height),
#                         xytext=(0, 3), textcoords="offset points",
#                         ha='center', va='bottom', fontsize=12, fontweight='bold', color='black')

#         # Rotate the x-axis labels to 45 degrees
#         plt.xticks(rotation=45, ha='right')

#         # Add horizontal line at y=0 for reference
#         ax.axhline(0, color='black', linewidth=0.8)

#         # Set background color to white
#         ax.set_facecolor('white')

#         # Add border around the plot
#         for spine in ax.spines.values():
#             spine.set_edgecolor('black')
#             spine.set_linewidth(1.2)

#         # Adjust layout
#         plt.tight_layout()

#         # Use Streamlit to display the chart
#         st.pyplot(fig)

# # SQL query to fetch the data for Active Straight Bets
# straight_bets_query = """
# WITH BaseQuery AS (
#     SELECT l.EventType, 
#            l.ParticipantName, 
#            ROUND(SUM(b.DollarsAtStake)) AS TotalDollarsAtStake, 
#            ROUND(SUM(b.PotentialPayout)) AS TotalPotentialPayout,
#            (SUM(b.DollarsAtStake) / SUM(b.PotentialPayout)) * 100 AS ImpliedProbability
#     FROM bets b    
#     JOIN legs l ON b.WagerID = l.WagerID
#     WHERE b.LegCount = 1
#       AND l.LeagueName = 'MLB'
#       AND b.WhichFund = 'GreenAleph'
#       AND b.WLCA = 'Active'
#     GROUP BY l.EventType, l.ParticipantName
    
#     UNION ALL
    
#     SELECT l.EventType, 
#            'Total by EventType' AS ParticipantName, 
#            ROUND(SUM(b.DollarsAtStake)) AS TotalDollarsAtStake, 
#            NULL AS TotalPotentialPayout,
#            (SUM(b.DollarsAtStake) / SUM(b.PotentialPayout)) * 100 AS ImpliedProbability
#     FROM bets b
#     JOIN legs l ON b.WagerID = l.WagerID
#     WHERE b.LegCount = 1
#       AND l.LeagueName = 'MLB'
#       AND b.WhichFund = 'GreenAleph'
#       AND b.WLCA = 'Active'
#     GROUP BY l.EventType

#     UNION ALL

#     SELECT NULL AS EventType, 
#            'Cumulative Total' AS ParticipantName, 
#            ROUND(SUM(b.DollarsAtStake)) AS TotalDollarsAtStake, 
#            NULL AS TotalPotentialPayout,
#            (SUM(b.DollarsAtStake) / SUM(b.PotentialPayout)) * 100 AS ImpliedProbability
#     FROM bets b
#     JOIN legs
#      l ON b.WagerID = l.WagerID
#     WHERE b.LegCount = 1
#       AND l.LeagueName = 'MLB'
#       AND b.WhichFund = 'GreenAleph'
#       AND b.WLCA = 'Active'
# )

# SELECT EventType, 
#        ParticipantName, 
#        FORMAT(TotalDollarsAtStake, 0) AS TotalDollarsAtStake, 
#        FORMAT(TotalPotentialPayout, 0) AS TotalPotentialPayout,
#        CONCAT(FORMAT(ImpliedProbability, 2), '%') AS ImpliedProbability
# FROM (
#     SELECT *, 
#            ROW_NUMBER() OVER (PARTITION BY EventType ORDER BY (ParticipantName = 'Total by EventType') ASC, ParticipantName) AS RowNum
#     FROM BaseQuery
# ) AS SubQuery
# ORDER BY EventType, RowNum;
# """

# # SQL query to fetch the data for Active Parlay Bets
# parlay_bets_query = """
# SELECT 
#     l.LegID,
#     l.EventType,
#     l.ParticipantName,
#     b.DollarsAtStake,
#     b.PotentialPayout,
#     b.ImpliedOdds,
#     l.EventLabel,
#     l.LegDescription
# FROM 
#     bets b
# JOIN 
#     legs l ON b.WagerID = l.WagerID
# WHERE 
#     l.LeagueName = 'MLB'
#     AND b.WhichFund = 'GreenAleph'
#     AND b.WLCA = 'Active'
#     AND b.LegCount > 1;
# """

# # Fetch the data for Active Straight Bets
# straight_bets_data = get_data_from_db(straight_bets_query)

# # Fetch the data for Active Parlay Bets
# parlay_bets_data = get_data_from_db(parlay_bets_query)

# # Display the data
# if straight_bets_data:
#     straight_bets_df = pd.DataFrame(straight_bets_data)
#     st.subheader('Active Straight Bets in GreenAleph Fund')
#     st.table(straight_bets_df)

# if parlay_bets_data:
#     parlay_bets_df = pd.DataFrame(parlay_bets_data)
#     st.subheader('Active Parlay Bets in GreenAleph Fund')
#     st.table(parlay_bets_df)
