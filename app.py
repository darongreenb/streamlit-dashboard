# cd /Users/aceyvogelstein/Bet_Housing_Database
# conda activate streamlit_env
# streamlit run app.py

import streamlit as st
import requests
import pandas as pd
import matplotlib.pyplot as plt

# API Gateway URL
api_url = "https://kaipkuyuf1.execute-api.us-east-1.amazonaws.com/prod/fetch_data"

# Fetch data from API
response = requests.get(api_url)
data = response.json()

# Convert the data to a DataFrame
df = pd.DataFrame(data)

# Streamlit App
st.title('My First Streamlit App')
st.write('Hello, world!')

st.title('Interactive GreenAleph Principal Dashboard')

# Sidebar for user input
st.sidebar.header('Filter Options')

# User input widgets
fund_option = st.sidebar.selectbox('Select Fund', ['GreenAleph', 'AnotherFund'])
status_option = st.sidebar.selectbox('Select Status', ['Active', 'Inactive', 'All'])

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
