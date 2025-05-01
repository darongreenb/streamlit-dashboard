import streamlit as st
import mysql.connector
import pandas as pd

# ──────────────── CONFIG ────────────────
st.set_page_config(page_title="NBA Futures – EV Table", layout="wide")

# ──────────────── DB CREDS ────────────────
BETTING_DB = {
    "host": "betting-db.cp86ssaw6cm7.us-east-1.rds.amazonaws.com",
    "user": "admin",
    "password": "7nRB1i2&A-K>",
    "database": "betting_db"
}

# ──────────────── MAIN APP ────────────────
def main():
    st.title("NBA Futures – EV Table")

    # Connect to database
    conn = mysql.connector.connect(**BETTING_DB)

    # Pull Active Bets
    query_active = """
        SELECT b.WagerID, b.PotentialPayout, b.DollarsAtStake,
               l.EventType, l.EventLabel, l.ParticipantName
        FROM bets b
        JOIN legs l ON b.WagerID = l.WagerID
        WHERE b.WhichBankroll = 'GreenAleph'
          AND b.WLCA = 'Active'
          AND l.LeagueName = 'NBA'
    """
    df_active = pd.read_sql(query_active, conn)

    # Aggregate active stake and payout by market
    active_dict = {}
    for _, row in df_active.iterrows():
        key = (row["EventType"], row["EventLabel"])
        if key not in active_dict:
            active_dict[key] = {"stake": 0, "payout": 0}
        active_dict[key]["stake"] += row["DollarsAtStake"]
        active_dict[key]["payout"] += row["PotentialPayout"]

    # Pull Resolved Bets
    query_resolved = """
        SELECT b.WagerID, b.NetProfit,
               l.EventType, l.EventLabel, l.ParticipantName
        FROM bets b
        JOIN legs l ON b.WagerID = l.WagerID
        WHERE b.WhichBankroll = 'GreenAleph'
          AND b.WLCA IN ('Win','Loss','Cashout')
          AND l.LeagueName = 'NBA'
    """
    df_resolved = pd.read_sql(query_resolved, conn)
    conn.close()

    # Aggregate net profit by market
    resolved_dict = {}
    for _, row in df_resolved.iterrows():
        key = (row["EventType"], row["EventLabel"])
        resolved_dict[key] = resolved_dict.get(key, 0) + row["NetProfit"]

    # Merge and calculate EV
    rows = []
    all_keys = set(active_dict.keys()) | set(resolved_dict.keys())
    for key in sorted(all_keys):
        stake = active_dict.get(key, {}).get("stake", 0)
        payout = active_dict.get(key, {}).get("payout", 0)
        net = resolved_dict.get(key, 0)
        ev = payout - stake + net
        rows.append({
            "EventType": key[0],
            "EventLabel": key[1],
            "ActiveDollarsAtStake": round(stake, 2),
            "ActiveExpectedPayout": round(payout, 2),
            "RealizedNetProfit": round(net, 2),
            "ExpectedValue": round(ev, 2)
        })

    st.dataframe(pd.DataFrame(rows), use_container_width=True)

# ──────────────── RUN ────────────────
if __name__ == "__main__":
    main()
