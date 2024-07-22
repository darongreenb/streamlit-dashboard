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
def get_data_from_db(query):
    try:
        conn = mysql.connector.connect(
            host=db_host,
            user=db_user,
            password=db_password,
            database=db_name
        )
        cursor = conn.cursor(dictionary=True)
        cursor.execute(query)
        data = cursor.fetchall()
        cursor.close()
        conn.close()
        return data
    except mysql.connector.Error as err:
        st.error(f"Error: {err}")
        return None

# Streamlit App
st.title('Interactive GreenAleph Principal Dashboard')

# Sidebar for user input
st.sidebar.header('Filter Options')

# SQL query to get unique funds and statuses
fund_query = "SELECT DISTINCT WhichFund FROM bets"
status_query = "SELECT DISTINCT WLCA FROM bets"

# Fetch unique fund and status options
fund_options = [item['WhichFund'] for item in get_data_from_db(fund_query)]
status_options = [item['WLCA'] for item in get_data_from_db(status_query)]

# User input widgets
fund_option = st.sidebar.selectbox('Select Fund', fund_options)
status_option = st.sidebar.selectbox('Select Status', status_options)

# SQL query to fetch filtered data
data_query = f"""
SELECT DollarsAtStake
FROM bets
WHERE WhichFund = '{fund_option}'
  AND WLCA = '{status_option}'
"""

# Fetch the filtered data
filtered_data = get_data_from_db(data_query)

# Check if data is fetched successfully
if filtered_data is None:
    st.error("Failed to fetch data from the database.")
else:
    # Calculate total dollars at stake based on filtered data
    total_dollars_at_stake = sum([item['DollarsAtStake'] for item in filtered_data])

    # Display the fetched data
    st.subheader(f'Total Dollars At Stake for {fund_option} ({status_option})')
    st.write(f'${total_dollars_at_stake:,.2f}')

    # Create data for visualization
    data = {'Category': ['Total Dollars At Stake'], 'Amount': [total_dollars_at_stake]}
    visual_df = pd.DataFrame(data)

    # Plot the bar chart
    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.bar(visual_df['Category'], visual_df['Amount'], color='lightgreen', width=0.2)
    ax.axhline(y=500000, color='green', linestyle='--', label='$500k Tranche')
    ax.set_title(f'Total Dollars At Stake ({fund_option}, {status_option})', fontsize=16)
    ax.set_xticks(visual_df['Category'])
    ax.tick_params(axis='x', rotation=0, labelsize=12)
    ax.tick_params(axis='y', labelsize=12)

    # Annotate the bar with the value
    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width() / 2.0, height, f'${height:,.2f}', ha='center', va='bottom', fontsize=12, color='black')

    # Add legend
    ax.legend()

    # Use Streamlit to display the chart
    st.pyplot(fig)

    # Display raw data in a table
    st.subheader('Raw Data')
    st.table(filtered_data)
