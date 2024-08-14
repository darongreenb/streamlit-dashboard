

# Sidebar for navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["GreenAleph Active Principal", "MLB Charts", "MLB Principal Tables", "MLB Participant Positions", "Profit", "Tennis Charts"])






elif page == "Tennis Charts":
    
    st.title('Tennis Active Bets - GA1')

    # Function to fetch and plot bar charts
    def plot_bar_chart(data, title, ylabel):
        df = pd.DataFrame(data)
        df['TotalDollarsAtStake'] = df['TotalDollarsAtStake'].astype(float).round(0)
        df = df.sort_values('TotalDollarsAtStake', ascending=True)

        # Define pastel colors
        pastel_colors = ['#a0d8f1', '#f4a261', '#e76f51', '#8ecae6', '#219ebc', '#023047', '#ffb703', '#fb8500', '#d4a5a5', '#9ab0a8']

        fig, ax = plt.subplots(figsize=(15, 10))
        bars = ax.bar(df['EventLabel'], df['TotalDollarsAtStake'],
                      color=[pastel_colors[i % len(pastel_colors)] for i in range(len(df['EventLabel']))],
                      width=0.6, edgecolor='black')

        ax.set_title(title, fontsize=18, fontweight='bold')
        ax.set_ylabel(ylabel, fontsize=14, fontweight='bold')

        # Annotate each bar with the value
        for bar in bars:
            height = bar.get_height()
            ax.annotate(f'{height:,.0f}', xy=(bar.get_x() + bar.get_width() / 2, height),
                        xytext=(0, 3), textcoords="offset points",
                        ha='center', va='bottom', fontsize=14, fontweight='bold', color='black')

        plt.xticks(rotation=45, ha='right', fontsize=14, fontweight='bold')
        ax.axhline(0, color='black', linewidth=0.8)
        ax.set_facecolor('white')
        for spine in ax.spines.values():
            spine.set_edgecolor('black')
            spine.set_linewidth(1.2)
        plt.tight_layout()
        st.pyplot(fig)

    # Filter for LeagueName
    league_name = st.selectbox('Select League', ['ATP', 'WTA'])

    # SQL query for EventLabel breakdown
    event_label_query = f"""
    WITH DistinctBets AS (
        SELECT DISTINCT WagerID, DollarsAtStake
        FROM bets
        WHERE WhichBankroll = 'GreenAleph'
          AND WLCA = 'Active'
    )
    SELECT 
        l.EventLabel,
        ROUND(SUM(db.DollarsAtStake), 0) AS TotalDollarsAtStake
    FROM 
        DistinctBets db
    JOIN 
        legs l ON db.WagerID = l.WagerID
    WHERE
        l.LeagueName = '{league_name}'
    GROUP BY 
        l.EventLabel;
    """

    event_label_data = get_data_from_db(event_label_query)
    if event_label_data is None:
        st.error("Failed to fetch EventLabel data.")
    else:
        plot_bar_chart(event_label_data, f'Total Active Principal by EventLabel ({league_name})', 'Total Dollars At Stake ($)')

        # Filter for EventLabel
        event_labels = sorted(set(row['EventLabel'] for row in event_label_data))
        event_label_option = st.selectbox('Select EventLabel', event_labels)

        if event_label_option:
            # Filter for EventType
            event_type_query = f"""
            SELECT DISTINCT l.EventType
            FROM 
                bets b
            JOIN 
                legs l ON b.WagerID = l.WagerID
            WHERE
                l.LeagueName = '{league_name}'
                AND l.EventLabel = '{event_label_option}'
                AND b.WhichBankroll = 'GreenAleph'
                AND b.WLCA = 'Active';
            """
            event_type_data = get_data_from_db(event_type_query)
            if event_type_data is None:
                st.error("Failed to fetch EventType data.")
            else:
                event_types = sorted(set(row['EventType'] for row in event_type_data))
                event_type_option = st.selectbox('Select EventType', event_types)

                if event_type_option:
                    # Query for combined chart (DollarsAtStake and PotentialPayout)
                    combined_query = f"""
                    WITH DistinctBets AS (
                        SELECT DISTINCT WagerID, DollarsAtStake, PotentialPayout
                        FROM bets
                        WHERE WhichBankroll = 'GreenAleph'
                          AND WLCA = 'Active'
                          AND LegCount = 1
                    )
                    SELECT 
                        l.ParticipantName,
                        SUM(db.DollarsAtStake) AS TotalDollarsAtStake,
                        SUM(db.PotentialPayout) AS TotalPotentialPayout
                    FROM 
                        DistinctBets db
                    JOIN 
                        legs l ON db.WagerID = l.WagerID
                    WHERE
                        l.LeagueName = '{league_name}'
                        AND l.EventLabel = '{event_label_option}'
                        AND l.EventType = '{event_type_option}'
                    GROUP BY 
                        l.ParticipantName;
                    """

                    combined_data = get_data_from_db(combined_query)
                    if combined_data is None:
                        st.error("Failed to fetch combined data.")
                    else:
                        df = pd.DataFrame(combined_data)
                        if not df.empty:
                            df['TotalDollarsAtStake'] = df['TotalDollarsAtStake'].astype(float).round(0)
                            df['TotalPotentialPayout'] = df['TotalPotentialPayout'].astype(float).round(0)
                            df = df.sort_values('TotalDollarsAtStake', ascending=True)

                            color_dollars_at_stake = '#219ebc'
                            color_potential_payout = '#f4a261'

                            fig, ax = plt.subplots(figsize=(18, 12))
                            bars1 = ax.bar(df['ParticipantName'], df['TotalDollarsAtStake'], color=color_dollars_at_stake, width=0.4, edgecolor='black', label='Total Dollars At Stake')
                            bars2 = ax.bar(df['ParticipantName'], df['TotalPotentialPayout'], color=color_potential_payout, width=0.4, edgecolor='black', label='Total Potential Payout', alpha=0.6, bottom=df['TotalDollarsAtStake'])

                            ax.set_ylabel('Total Amount ($)', fontsize=16, fontweight='bold')
                            ax.set_title(f'Total Active Principal Overlaid on Potential Payout by ParticipantName for {event_type_option} - {event_label_option} ({league_name}, Straight Bets Only)', fontsize=18, fontweight='bold')

                            for bar1 in bars1:
                                height = bar1.get_height()
                                ax.annotate(f'{height:,.0f}', xy=(bar1.get_x() + bar1.get_width() / 2, height),
                                            xytext=(0, 3), textcoords="offset points",
                                            ha='center', va='bottom', fontsize=12, fontweight='bold', color='black')

                            for bar1, bar2 in zip(bars1, bars2):
                                height1 = bar1.get_height()
                                height2 = bar2.get_height()
                                total_height = height1 + height2
                                ax.annotate(f'{height2:,.0f}', 
                                            xy=(bar2.get_x() + bar2.get_width() / 2, total_height),
                                            xytext=(0, 3), textcoords="offset points",
                                            ha='center', va='bottom', fontsize=12, fontweight='bold', color='black')

                            plt.xticks(rotation=45, ha='right', fontsize=14, fontweight='bold')
                            ax.axhline(0, color='black', linewidth=0.8)
                            ax.set_facecolor('white')
                            for spine in ax.spines.values():
                                spine.set_edgecolor('black')
                                spine.set_linewidth(1.2)
                            ax.legend()
                            plt.tight_layout()
                            st.pyplot(fig)
                        else:
                            st.error("No data available for the selected filters.")

 
