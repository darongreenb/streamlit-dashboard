import streamlit as st
import mysql.connector
import pandas as pd

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

# Streamlit App
st.title('MLB Bets and Legs Viewer for GreenAleph Fund')

# Fetch unique participant names for the dropdown filter
participant_query = """
SELECT DISTINCT l.ParticipantName
FROM bets b
JOIN legs l ON b.WagerID = l.WagerID
WHERE b.WhichFund = 'Beta' AND l.LeagueName = 'MLB'
"""
participants = [item['ParticipantName'] for item in get_data_from_db(participant_query)]

# Participant name filter
participant_name = st.selectbox('Select Participant Name', participants)

# WLCA sub-filter
wlca_options = ['Win', 'Loss', 'Cashout', 'Active']
wlca_filter = st.selectbox('Select WLCA Status', wlca_options)

# Straight or Parlay sub-filter
bet_type = st.selectbox('Select Bet Type', ['Straight', 'Parlay'])

# Determine LegCount condition based on bet type
if bet_type == 'Straight':
    leg_count_condition = "b.LegCount = 1"
else:
    leg_count_condition = "b.LegCount > 1"

# SQL query to fetch the filtered data
filtered_query = f"""
SELECT 
    b.*, 
    l.*
FROM 
    bets b
JOIN 
    legs l ON b.WagerID = l.WagerID
WHERE 
    l.ParticipantName = %s
    AND b.WLCA = %s
    AND {leg_count_condition}
    AND b.WhichFund = 'Beta'
    AND l.LeagueName = 'MLB'
"""

# Fetch the filtered data
filtered_data = get_data_from_db(filtered_query, (participant_name, wlca_filter))

# Check if data is fetched successfully
if filtered_data is None:
    st.error("Failed to fetch data from the database.")
elif len(filtered_data) == 0:
    st.write("No data found for the selected filters.")
else:
    # Create a DataFrame from the fetched data
    filtered_df = pd.DataFrame(filtered_data)

    # Display the fetched data
    st.subheader(f'Bets and Legs for {participant_name} ({wlca_filter} - {bet_type})')
    st.dataframe(filtered_df)
