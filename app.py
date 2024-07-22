import streamlit as st
import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt

# Set up the connection to the database
def get_db_connection():
    conn = mysql.connector.connect(
        host=st.secrets["DB_HOST"],
        user=st.secrets["DB_USER"],
        password=st.secrets["DB_PASSWORD"],
        database=st.secrets["DB_NAME"]
    )
    return conn

# Streamlit App
st.title('GreenAleph Principal Dashboard')

# Sidebar for filter options
st.sidebar.header('Filter Options')
wlca_option = st.sidebar.selectbox('Select WLCA', ['Active', 'Win', 'Loss', 'Cashout'])

# Query the database
conn = get_db_connection()
query = f"""
SELECT SUM(b.DollarsAtStake) AS TotalDollarsAtStake
FROM bets b
JOIN legs l ON b.WagerID = l.WagerID
WHERE b.WhichFund = 'GreenAleph' AND b.WLCA = '{wlca_option}';
"""
total_dollars_at_stake = pd.read_sql(query, conn).iloc[0, 0]
conn.close()

# Create data for visualization
data = {'Category': ['Total Dollars At Stake'], 'Amount': [total_dollars_at_stake]}
df = pd.DataFrame(data)

# Plot the bar chart
fig, ax = plt.subplots(figsize=(10, 6))

# Plot the bar chart with a "battery" aesthetic
bars = ax.bar(df['Category'], df['Amount'], color='lightgreen', width=0.4)

# Draw the battery outline
for bar in bars:
    bar_x = bar.get_x()
    bar_width = bar.get_width()
    bar_height = bar.get_height()
    # Outline
    ax.add_patch(plt.Rectangle((bar_x - 0.05, 0), bar_width + 0.1, 500000, fill=None, edgecolor='black', linewidth=2))
    # Cap
    ax.add_patch(plt.Rectangle((bar_x + bar_width / 2 - 0.05, 500000), 0.1, 50000, fill='black'))

# Add the $500k tranche line
ax.axhline(y=500000, color='green', linestyle='--', label='$500k Tranche')

# Add labels and title
ax.set_title(f'Total Dollars At Stake ({wlca_option} - GreenAleph)', fontsize=16, fontweight='bold')
ax.set_xticks(df['Category'])
ax.set_xticklabels(df['Category'], fontsize=12)
ax.tick_params(axis='x', rotation=0, labelsize=12)
ax.tick_params(axis='y', labelsize=12)
ax.set_yticks(range(0, 600000, 100000))
ax.set_yticklabels([f'${y:,}' for y in range(0, 600000, 100000)], fontsize=12)

# Annotate the bar with the value
for bar in bars:
    height = bar.get_height()
    ax.text(bar.get_x() + bar.get_width() / 2.0, height, f'${height:,.2f}', ha='center', va='bottom', fontsize=12, color='black')

# Add legend
ax.legend()

# Set background color to white
ax.set_facecolor('white')

# Add border around the plot
for spine in ax.spines.values():
    spine.set_edgecolor('black')
    spine.set_linewidth(1.2)

# Adjust layout
plt.tight_layout()

# Show the plot in Streamlit
st.pyplot(fig)

# Display raw data in a table
st.subheader('Raw Data')
st.write(filtered_df)
