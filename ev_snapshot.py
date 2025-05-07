#!/usr/bin/env python3
"""
ev_snapshot.py
––––––––––––––––––––––––––––––––––––––––––––––
Runs build_ev_table(), pulls the TOTAL ExpectedValue,
and writes a snapshot to ev_history via save_snapshot().
Call it from cron, Lambda, or however you schedule jobs.
"""

from ev_helpers import save_snapshot          # <- the helpers you just added
from your_module import build_ev_table        # <- replace with the real module name

def main():
    # Build the table exactly as in your notebook / Streamlit app
    df = build_ev_table()

    # Extract TOTAL row’s ExpectedValue
    total_ev = float(
        df.loc[df["LeagueName"] == "TOTAL", "ExpectedValue"].iloc[0]
    )

    # Write (upsert) today’s snapshot
    save_snapshot(total_ev)

    # Optional confirmation when run manually
    print(f"Snapshot saved for today: ${total_ev:,.2f}")

if __name__ == "__main__":
    main()
