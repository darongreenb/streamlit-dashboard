elif page == "Profit":
    # Profit Page
    st.title('Realized Profit - GA1')

    # SQL query for the new bar chart (Profit by League)
    league_profit_query = """
    WITH DistinctBets AS (
        SELECT DISTINCT b.WagerID, b.NetProfit, l.LeagueName
        FROM bets b
        JOIN legs l ON b.WagerID = l.WagerID
        WHERE b.WhichBankroll = 'GreenAleph'
    ),
    LeagueSums AS (
        SELECT 
            l.LeagueName,
            ROUND(SUM(db.NetProfit), 0) AS NetProfit
        FROM 
            DistinctBets db
        JOIN 
            (SELECT DISTINCT WagerID, LeagueName FROM legs) l ON db.WagerID = l.WagerID
        GROUP BY 
            l.LeagueName
    )
    SELECT * FROM LeagueSums

    UNION ALL

    SELECT 
        'Total' AS LeagueName,
        ROUND(SUM(db.NetProfit), 0) AS NetProfit
    FROM 
        DistinctBets db;
    """

    # Fetch the data for the new bar chart
    league_profit_data = get_data_from_db(league_profit_query)
    if league_profit_data is None:
        st.error("Failed to fetch league profit data from the database.")
    else:
        # Create a DataFrame from the fetched data
        league_profit_df = pd.DataFrame(league_profit_data)
        
        # Create the bar chart
        fig, ax = plt.subplots(figsize=(15, 8))
        bar_colors = league_profit_df['NetProfit'].apply(lambda x: 'green' if x > 0 else 'red')  # Green for positive, Red for negative
        bars = ax.bar(league_profit_df['LeagueName'], league_profit_df['NetProfit'], color=bar_colors, edgecolor='black')

        # Adding titles and labels
        ax.set_title('Realized Profit by League', fontsize=18, fontweight='bold')
        ax.set_xlabel('League Name', fontsize=16, fontweight='bold')
        ax.set_ylabel('Realized Profit ($)', fontsize=16, fontweight='bold')

        # Annotate each bar with the value
        for bar in bars:
            height = bar.get_height()
            ax.annotate(f'${height:,.0f}', xy=(bar.get_x() + bar.get_width() / 2, height),
                        xytext=(0, 3), textcoords="offset points",
                        ha='center', va='bottom', fontsize=12, fontweight='bold', color='black')

        # Rotate the x-axis labels to 45 degrees
        plt.xticks(rotation=45, ha='right', fontsize=14, fontweight='bold')

        # Set background color to white
        ax.set_facecolor('white')
        plt.gcf().set_facecolor('white')

        # Add border around the plot
        for spine in ax.spines.values():
            spine.set_edgecolor('black')
            spine.set_linewidth(1.2)

        # Adjust y-axis range
        ymin = league_profit_df['NetProfit'].min() - 500
        ymax = league_profit_df['NetProfit'].max() + 500
        ax.set_ylim(ymin, ymax)

        # Adjust layout
        plt.tight_layout()

        # Use Streamlit to display the chart
        st.pyplot(fig)

    # Fetch the distinct LeagueName for the dropdown
    event_type_query = "SELECT DISTINCT LeagueName FROM legs ORDER BY LeagueName ASC;"
    event_types = get_data_from_db(event_type_query)

    if event_types is None:
        st.error("Failed to fetch event types from the database.")
    else:
        event_type_names = [event['LeagueName'] for event in event_types]
        event_type_names.insert(0, "All")  # Add "All" to the beginning of the list

        # Dropdown menu for selecting LeagueName
        selected_event_type = st.selectbox('Select LeagueName', event_type_names)

        # SQL query to fetch data, filter by LeagueName if not "All"
        if selected_event_type == "All":
            profit_query = """
            WITH DistinctBets AS (
                SELECT DISTINCT WagerID, NetProfit, DateTimePlaced
                FROM bets
                WHERE WhichBankroll = 'GreenAleph'
            )
            SELECT DateTimePlaced, SUM(NetProfit) AS NetProfit
            FROM DistinctBets
            GROUP BY DateTimePlaced
            ORDER BY DateTimePlaced;
            """
        else:
            profit_query = """
            WITH DistinctBets AS (
                SELECT DISTINCT b.WagerID, b.NetProfit, b.DateTimePlaced
                FROM bets b
                JOIN legs l ON b.WagerID = l.WagerID
                WHERE b.WhichBankroll = 'GreenAleph'
                  AND l.LeagueName = %s
            )
            SELECT DateTimePlaced, SUM(NetProfit) AS NetProfit
            FROM DistinctBets
            GROUP BY DateTimePlaced
            ORDER BY DateTimePlaced;
            """
            params = [selected_event_type]

        # Fetch the data
        data = get_data_from_db(profit_query, params if selected_event_type != "All" else None)

        if data is None:
            st.error("Failed to fetch data from the database.")
        else:
            df = pd.DataFrame(data)
            if 'DateTimePlaced' not in df.columns:
                st.error("The 'DateTimePlaced' column is missing from the data.")
            else:
                try:
                    # Ensure DateTimePlaced is a datetime object
                    df['DateTimePlaced'] = pd.to_datetime(df['DateTimePlaced'])

                    # Filter data to start from March 2024
                    df = df[df['DateTimePlaced'] >= '2024-03-01']

                    # Sort by DateTimePlaced
                    df.sort_values(by='DateTimePlaced', inplace=True)

                    # Calculate the cumulative net profit
                    df['Cumulative Net Profit'] = df['NetProfit'].cumsum()

                    # Drop initial zero values for line start
                    non_zero_index = df[df['Cumulative Net Profit'] != 0].index.min()
                    df = df.loc[non_zero_index:]

                    # Create the line chart
                    fig, ax = plt.subplots(figsize=(15, 10))

                    # Plot the line in black color
                    ax.plot(df['DateTimePlaced'], df['Cumulative Net Profit'], color='black', linewidth=4)

                    # Adding titles and labels
                    ax.set_title('Cumulative Realized Profit Over Time', fontsize=18, fontweight='bold')
                    ax.set_xlabel('Date of Bet Placed', fontsize=16, fontweight='bold')
                    ax.set_ylabel('USD ($)', fontsize=16, fontweight='bold')

                    # Annotate only the last data point with the value
                    last_point = df.iloc[-1]
                    ax.annotate(f'${last_point["Cumulative Net Profit"]:,.0f}', 
                                xy=(last_point['DateTimePlaced'], last_point['Cumulative Net Profit']),
                                xytext=(5, 5), textcoords="offset points",
                                ha='left', va='bottom', fontsize=12, fontweight='bold', color='black')

                    # Rotate the x-axis labels to 45 degrees
                    plt.xticks(rotation=30, ha='right', fontsize=14, fontweight='bold')

                    # Add horizontal line at y=0 for reference
                    ax.axhline(0, color='black', linewidth=1.5)

                    # Set x-axis and y-axis limits to ensure consistency
                    x_min = df['DateTimePlaced'].min()
                    x_max = df['DateTimePlaced'].max()
                    ymin = df['Cumulative Net Profit'].min() - 500
                    ymax = df['Cumulative Net Profit'].max() + 500
                    ax.set_xlim(x_min, x_max)
                    ax.set_ylim(ymin, ymax)

                    # Set background color to white
                    ax.set_facecolor('white')
                    plt.gcf().set_facecolor('white')

                    # Add border around the plot
                    for spine in ax.spines.values():
                        spine.set_edgecolor('black')
                        spine.set_linewidth(1.2)

                    # Adjust layout
                    plt.tight_layout()

                    # Use Streamlit to display the chart
                    st.pyplot(fig)
                except Exception as e:
                    st.error(f"Error processing data: {e}")

