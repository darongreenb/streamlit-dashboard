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

# SQL query to get unique WLCA statuses
status_query = "SELECT DISTINCT WLCA FROM bets"

# Fetch unique status options
status_options = [item['WLCA'] for item in get_data_from_db(status_query)]

# User input widget for WLCA status
status_option = st.sidebar.selectbox('Select Status', status_options)

# SQL query to fetch filtered data
data_query = f"""
SELECT 
    l.LeagueName,
    SUM(b.DollarsAtStake) AS TotalDollarsAtStake
FROM 
    (SELECT DISTINCT WagerID, DollarsAtStake
     FROM bets
     WHERE WhichFund = 'Beta'
       AND WLCA = '{status_option}') b
JOIN 
    legs l ON b.WagerID = l.WagerID
GROUP BY 
    l.LeagueName

UNION ALL

SELECT 
    'Total' AS LeagueName,
    SUM(b.DollarsAtStake) AS TotalDollarsAtStake
FROM 
    (SELECT DISTINCT WagerID, DollarsAtStake
     FROM bets
     WHERE WhichFund = 'Beta'
       AND WLCA = '{status_option}') b;
"""

# Fetch the filtered data
filtered_data = get_data_from_db(data_query)

# Check if data is fetched successfully
if filtered_data is None:
    st.error("Failed to fetch data from the database.")
else:
    # Create a DataFrame from the fetched data
    df = pd.DataFrame(filtered_data)

    # Display the fetched data
    st.subheader(f'Total Dollars At Stake for Beta Fund ({status_option})')

    # Display raw data in a table
    st.table(df)

    # Create data for visualization
    df['TotalDollarsAtStake'] = df['TotalDollarsAtStake'].astype(float)

    # Plot the bar chart
    fig, ax = plt.subplots(figsize=(12, 8))
    bars = ax.bar(df['LeagueName'], df['TotalDollarsAtStake'], color=['#6a0dad' if name == 'Total' else '#ffcccb' for name in df['LeagueName']], width=0.6, edgecolor='black')

    # Add labels and title
    ax.set_title('Total Dollars At Stake by LeagueName (Beta Fund)', fontsize=18, fontweight='bold')
    ax.set_ylabel('Total Dollars At Stake ($)', fontsize=14, fontweight='bold')

    # Annotate each bar with the value
    for bar in bars:
        height = bar.get_height()
        ax.annotate(f'${height:,.2f}', xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 3), textcoords="offset points",
                    ha='center', va='bottom', fontsize=12, fontweight='bold', color='black')

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
