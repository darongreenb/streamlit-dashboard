import streamlit as st
import mysql.connector
import pandas as pd

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
st.title('MLB 2024 Betting Event View (Beta Fund)')

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
      AND b.WhichFund = 'Beta'
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
      AND b.WhichFund = 'Beta'
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
      AND b.WhichFund = 'Beta'
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
    st.subheader('Active Straight Bets in Beta Fund')
    st.table(straight_bets_df)

if parlay_bets_data:
    parlay_bets_df = pd.DataFrame(parlay_bets_data)
    st.subheader('Active Parlay Bets in GreenAleph Fund')
    st.table(parlay_bets_df)
