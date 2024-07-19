import streamlit as st
import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt

st.title('My First Streamlit App')
st.write('Hello, world!')

st.title('Interactive GreenAleph Principal Dashboard')

# Sidebar for user input
st.sidebar.header('Filter Options')

# User input widgets
fund_option = st.sidebar.selectbox('Select Fund', ['GreenAleph', 'AnotherFund'])
status_option = st.sidebar.selectbox('Select Status', ['Active', 'Inactive', 'All'])

# Database connection function
def get_db_connection():
    conn = mysql.connector.connect(
        host='betting-db.cp86ssaw6cm7.us-east-1.rds.amazonaws.com',
        user='admin',
        password='7nRB1i2&A-K>',
        database='betting_db'
    )
    return conn

# Function to execute a query and return data as a single result
def fetch_single_result(query, params):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(query, params)
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    return float(result[0]) if result and result[0] is not None else 0.0

# SQL Query
query_total_dollars_at_stake = """
SELECT SUM(DISTINCT b.DollarsAtStake) AS TotalDollarsAtStake
FROM bets b
JOIN (
    SELECT WagerID
    FROM legs
    GROUP BY WagerID
    HAVING COUNT(*) > 1
) l ON b.WagerID = l.WagerID
WHERE b.WhichFund = %s
"""
if status_option != 'All':
    query_total_dollars_at_stake += " AND b.WLCA = %s"

# Fetch the data based on user input
params = [fund_option]
if status_option != 'All':
    params.append(status_option)
total_dollars_at_stake = fetch_single_result(query_total_dollars_at_stake, params)

# Display the fetched data
st.subheader(f'Total Dollars At Stake for {fund_option} ({status_option})')
st.write(f'${total_dollars_at_stake:,.2f}')

# Create data for visualization
data = {'Category': ['Total Dollars At Stake'], 'Amount': [total_dollars_at_stake]}
df = pd.DataFrame(data)

# Plot the bar chart
fig, ax = plt.subplots(figsize=(10, 6))
bars = ax.bar(df['Category'], df['Amount'], color='lightgreen', width=0.2)
ax.axhline(y=500000, color='green', linestyle='--', label='$500k Tranche')
ax.set_xlabel('Category', fontsize=14)
ax.set_ylabel('Amount in $', fontsize=14)
ax.set_title(f'Total Dollars At Stake ({fund_option}, {status_option})', fontsize=16)
ax.set_xticks(df['Category'])
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
st.table(df)