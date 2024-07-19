import streamlit as st
import requests
import pandas as pd
import matplotlib.pyplot as plt
import boto3
from requests.auth import AuthBase
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest

# API Gateway URL from secrets
api_url = st.secrets["API_URL"]

# Retrieve AWS credentials from secrets
aws_access_key_id = st.secrets["AWS_ACCESS_KEY_ID"]
aws_secret_access_key = st.secrets["AWS_SECRET_ACCESS_KEY"]

# Custom AWS Auth class to sign requests
class AWSV4Auth(AuthBase):
    def __init__(self, service, region):
        self.service = service
        self.region = region
        self.credentials = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        ).get_credentials()
        self.signer = SigV4Auth(self.credentials, self.service, self.region)

    def __call__(self, r):
        aws_request = AWSRequest(method=r.method, url=r.url, data=r.body)
        self.signer.add_auth(aws_request)
        r.headers.update(aws_request.headers.items())
        return r

# Create AWS V4 Auth
auth = AWSV4Auth('execute-api', 'us-east-1')

# Fetch data from API
response = requests.get(api_url, auth=auth)
data = response.json()

# Debugging: Print the response data to understand its structure
st.write("Response data:", data)

# Ensure data is a list of dictionaries before converting to DataFrame
if isinstance(data, list) and all(isinstance(item, dict) for item in data):
    # Convert the data to a DataFrame
    df = pd.DataFrame(data)
else:
    st.error("Unexpected data format received from API.")
    st.stop()

# Streamlit App
st.title('My First Streamlit App')
st.write('Hello, world!')

st.title('Interactive GreenAleph Principal Dashboard')

# Sidebar for user input
st.sidebar.header('Filter Options')

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
