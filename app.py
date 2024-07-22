import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import mysql.connector

# Retrieve secrets from Streamlit
db_host = st.secrets["DB_HOST"]
db_user = st.secrets["DB_USER"]
db_password = st.secrets["DB_PASSWORD"]
db_name = st.secrets["DB_NAME"]

# Connect to the database
def get_db_connection():
    conn = mysql.connector.connect(
        host=db_host,
        user=db_user,
        password=db_password,
        database=db_name
    )
    return conn

# Streamlit App
st.title('Interactive GreenAleph Principal Dashboard')

# Sidebar for user input
st.sidebar.header('Filter Options')

# Fetch data from database
conn = get_db_connection()
query = "SELECT * FROM your_table_name"  # Replace 'your_table_name' with your actual table name
df = pd.read_sql(query, conn)
conn.close()

# Ensure data is in the expected format
if df.empty:
    st.error("No data fetched from the database.")
    st.stop()

# User input widgets
fund_option = st.sidebar.selectbox('Select Fund', df['fund'].unique())
status_option = st.sidebar.selectbox('Select Status', df['status'].unique())

# Filter the fetched data based on user input
filtered_df = df[(df['fund'] == fund_option) & (df['status'] == status_option)]

# Calculate total dollars at stake based on filtered data
total_dollars_at_stake = filtered_df['DollarsAtStake'].sum()

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
ax.set_xlabel('Category', fontsize=14)
ax.set_ylabel('Amount in $', fontsize=14)
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
st.table(filtered_df)
