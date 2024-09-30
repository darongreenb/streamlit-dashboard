import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import copy

# Retrieve secrets from Streamlit (Example setup)
db_host = st.secrets["DB_HOST"]
db_user = st.secrets["DB_USER"]
db_password = st.secrets["DB_PASSWORD"]
db_name = st.secrets["DB_NAME"]

# Define the probabilities and payouts for each team in the AFC and NFC
teams_probs = [
    # AFC Teams
    {'team': 'Team 1', 'seed': 1, 'conference': 'AFC', 'win_conference': 0.30, 'win_championship': 0.18, 'payout_conference': 5000, 'payout_championship': 20000},
    {'team': 'Team 2', 'seed': 2, 'conference': 'AFC', 'win_conference': 0.25, 'win_championship': 0.15, 'payout_conference': 6000, 'payout_championship': 25000},
    {'team': 'Team 3', 'seed': 3, 'conference': 'AFC', 'win_conference': 0.15, 'win_championship': 0.08, 'payout_conference': 7000, 'payout_championship': 30000},
    {'team': 'Team 4', 'seed': 4, 'conference': 'AFC', 'win_conference': 0.10, 'win_championship': 0.05, 'payout_conference': 8000, 'payout_championship': 35000},
    {'team': 'Team 5', 'seed': 5, 'conference': 'AFC', 'win_conference': 0.08, 'win_championship': 0.04, 'payout_conference': 9000, 'payout_championship': 40000},
    {'team': 'Team 6', 'seed': 6, 'conference': 'AFC', 'win_conference': 0.07, 'win_championship': 0.03, 'payout_conference': 10000, 'payout_championship': 45000},
    {'team': 'Team 7', 'seed': 7, 'conference': 'AFC', 'win_conference': 0.05, 'win_championship': 0.02, 'payout_conference': 12000, 'payout_championship': 50000},
    # NFC Teams
    {'team': 'Team 8', 'seed': 8, 'conference': 'NFC', 'win_conference': 0.28, 'win_championship': 0.15, 'payout_conference': 5000, 'payout_championship': 20000},
    {'team': 'Team 9', 'seed': 9, 'conference': 'NFC', 'win_conference': 0.22, 'win_championship': 0.12, 'payout_conference': 6000, 'payout_championship': 25000},
    {'team': 'Team 10', 'seed': 10, 'conference': 'NFC', 'win_conference': 0.15, 'win_championship': 0.07, 'payout_conference': 7000, 'payout_championship': 30000},
    {'team': 'Team 11', 'seed': 11, 'conference': 'NFC', 'win_conference': 0.12, 'win_championship': 0.06, 'payout_conference': 8000, 'payout_championship': 35000},
    {'team': 'Team 12', 'seed': 12, 'conference': 'NFC', 'win_conference': 0.09, 'win_championship': 0.05, 'payout_conference': 9000, 'payout_championship': 40000},
    {'team': 'Team 13', 'seed': 13, 'conference': 'NFC', 'win_conference': 0.08, 'win_championship': 0.04, 'payout_conference': 10000, 'payout_championship': 45000},
    {'team': 'Team 14', 'seed': 14, 'conference': 'NFC', 'win_conference': 0.06, 'win_championship': 0.03, 'payout_conference': 12000, 'payout_championship': 50000}
]

# Placeholder function to calculate EV for a given outcome (higher seed wins or lower seed wins)
def calculate_total_ev(teams_probs):
    ev_conference = sum(team['win_conference'] * team['payout_conference'] for team in teams_probs)
    ev_championship = sum(team['win_championship'] * team['payout_championship'] for team in teams_probs)
    return ev_conference + ev_championship

# Function to calculate EV delta for all first-round matchups
def calculate_ev_deltas_for_all_matchups(teams_probs):
    matchups = [
        # AFC matchups
        ('AFC', 2, 7), ('AFC', 3, 6), ('AFC', 4, 5),
        # NFC matchups
        ('NFC', 9, 14), ('NFC', 10, 13), ('NFC', 11, 12)
    ]
    
    ev_deltas = {}
    
    for conference, seed1, seed2 in matchups:
        original_probs = copy.deepcopy(teams_probs)
        
        # Scenario 1: Higher seed wins
        # Placeholder logic - Assume higher seed wins EV increases by 10% of potential payout
        ev_higher_seed_wins = calculate_total_ev(original_probs)
        
        # Scenario 2: Lower seed wins
        # Placeholder logic - Assume lower seed wins EV decreases by 10% of potential payout
        ev_lower_seed_wins = calculate_total_ev(original_probs) * 0.9
        
        ev_deltas[(conference, seed1, seed2)] = abs(ev_higher_seed_wins - ev_lower_seed_wins)
    
    return ev_deltas

# Calculate EV Deltas
ev_deltas = calculate_ev_deltas_for_all_matchups(teams_probs)

# Prepare data for bar chart using the provided EV Delta values
matchup_labels = [
    "AFC 2 vs 7", "AFC 3 vs 6", "AFC 4 vs 5",
    "NFC 2 vs 7", "NFC 3 vs 6", "NFC 4 vs 5"
]
ev_delta_values = [
    1658, 586, 196,
    1934, 889, 269
]

# Create a DataFrame for easier plotting
ev_deltas_df = pd.DataFrame({
    'Matchup': matchup_labels,
    'EV Delta': ev_delta_values
})

# Sort by EV Delta for better visualization
ev_deltas_df.sort_values(by='EV Delta', ascending=False, inplace=True)

# Plotting the EV Deltas as a vertical bar chart
st.title("NFL Playoff Matchups: Wild Card Round")
fig, ax = plt.subplots()
ax.bar(ev_deltas_df['Matchup'], ev_deltas_df['EV Delta'], color='skyblue')
ax.set_ylabel("USD ($)")
ax.set_title("EV Δ by Matchup")
plt.xticks(rotation=45, ha='right')  # Rotate the x-axis labels for better readability

# Display the bar chart in Streamlit
st.pyplot(fig)


