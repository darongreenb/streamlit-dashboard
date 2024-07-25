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
st.title('MLB Active Principal in GreenAleph Fund')

# SQL query to fetch data for the main bar chart
main_query = """
WITH DistinctBets AS (
    SELECT DISTINCT WagerID, DollarsAtStake
    FROM bets
    WHERE WhichFund = 'GreenAleph'
      AND WLCA = 'Active'
)
SELECT 
    l.EventType,
    SUM(b.DollarsAtStake) AS TotalDollarsAtStake
FROM 
    DistinctBets b
JOIN 
    legs l ON b.WagerID = l.WagerID
WHERE
    l.LeagueName = 'MLB'
GROUP BY 
    l.EventType

UNION ALL

SELECT 
    'Total' AS EventType,
    SUM(b.DollarsAtStake) AS TotalDollarsAtStake
FROM 
    DistinctBets b
JOIN 
    legs l ON b.WagerID = l.WagerID
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

    # Remove the "Total" filter option
    main_df = main_df[main_df['EventType'] != 'Total']

    # Display the fetched data
    st.subheader('Total Dollars At Stake by EventType (GreenAleph Fund)')
    
    # Create data for visualization
    main_df['TotalDollarsAtStake'] = main_df['TotalDollarsAtStake'].astype(float)

    # Plot the main bar chart
    fig, ax = plt.subplots(figsize=(12, 8))
    bars = ax.bar(main_df['EventType'], main_df['TotalDollarsAtStake'], color=['#a0d8f1', '#f4a261', '#e76f51', '#8ecae6', '#219ebc', '#023047', '#ffb703', '#fb8500'], width=0.6, edgecolor='black')

    # Add labels and title
    ax.set_title('GreenAleph Fund: Total Active Principal', fontsize=18, fontweight='bold')
    ax.set_ylabel('Total Dollars At Stake ($)', fontsize=14, fontweight='bold')

    # Annotate each bar with the value
    for bar in bars:
        height = bar.get_height()
        ax.annotate(f'${height:,.2f}', xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 3), textcoords="offset points",
                    ha='center', va='bottom', fontsize=12, fontweight='bold', color='black')

    # Rotate the x-axis labels vertically
    plt.xticks(rotation=90)

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

    # Add filter for EventType
    event_type_option = st.selectbox('Select EventType', main_df['EventType'].unique())

    # SQL query to fetch data for the filtered bar chart
    filtered_query = f"""
    WITH DistinctBets AS (
        SELECT DISTINCT WagerID, DollarsAtStake
        FROM bets
        WHERE WhichFund = 'GreenAleph'
          AND WLCA = 'Active'
    )
    SELECT 
        l.ParticipantName,
        SUM(b.DollarsAtStake) AS TotalDollarsAtStake
    FROM 
        DistinctBets b
    JOIN 
        legs l ON b.WagerID = l.WagerID
    WHERE
        l.LeagueName = 'MLB'
        AND l.EventType = '{event_type_option}'
    GROUP BY 
        l.ParticipantName;
    """

    # Fetch the filtered data
    filtered_data = get_data_from_db(filtered_query)

    # Check if data is fetched successfully
    if filtered_data is None:
        st.error("Failed to fetch data from the database.")
    else:
        # Create a DataFrame from the fetched data
        filtered_df = pd.DataFrame(filtered_data)

        # Display the fetched data
        st.subheader(f'Total Dollars At Stake by ParticipantName for {event_type_option} (GreenAleph Fund)')

        # Create data for visualization
        filtered_df['TotalDollarsAtStake'] = filtered_df['TotalDollarsAtStake'].astype(float)

        # Plot the filtered bar chart
        fig, ax = plt.subplots(figsize=(12, 8))
        bars = ax.bar(filtered_df['ParticipantName'], filtered_df['TotalDollarsAtStake'], color='#a0d8f1', width=0.6, edgecolor='black')

        # Add labels and title
        ax.set_title(f'Total Active Principal by ParticipantName for {event_type_option}', fontsize=18, fontweight='bold')
        ax.set_ylabel('Total Dollars At Stake ($)', fontsize=14, fontweight='bold')

        # Annotate each bar with the value
        for bar in bars:
            height = bar.get_height()
            ax.annotate(f'${height:,.2f}', xy=(bar.get_x() + bar.get_width() / 2, height),
                        xytext=(0, 3), textcoords="offset points",
                        ha='center', va='bottom', fontsize=12, fontweight='bold', color='black')

        # Rotate the x-axis labels vertically
        plt.xticks(rotation=90)

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
