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
st.title('GreenAleph Principal Dashboard')

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
    ROUND(SUM(DollarsAtStake), 0) AS TotalDollarsAtStake
FROM 
    DistinctBets db
JOIN 
    (SELECT DISTINCT WagerID, LeagueName FROM legs) l ON db.WagerID = l.WagerID
GROUP BY 
    l.LeagueName

UNION ALL

SELECT 
    'Total' AS LeagueName,
    ROUND(SUM(DollarsAtStake), 0) AS TotalDollarsAtStake
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
    st.subheader(f'Total Dollars At Stake for GreenAleph Fund (Active)')

    # Display raw data in a table
    st.table(df)

    # Define colors for bars
    colors = ['#77dd77', '#89cff0', '#fdfd96', '#ffb347', '#aec6cf', '#cfcfc4', '#ffb6c1', '#b39eb5']
    total_color = '#006400'  # Dark green for the Total bar

    # Create color list ensuring 'Total' bar is dark green
    bar_colors = [total_color if name == 'Total' else colors[i % len(colors)] for i, name in enumerate(df['LeagueName'])]

    # Plot the bar chart
    fig, ax = plt.subplots(figsize=(12, 8))
    bars = ax.bar(df['LeagueName'], df['TotalDollarsAtStake'], color=bar_colors, width=0.6, edgecolor='black')

    # Add labels and title
    ax.set_title('GreenAleph Fund: Total Active Principal', fontsize=18, fontweight='bold')
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
