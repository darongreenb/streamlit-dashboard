# ────────────────────────────────────  NBA Futures Dashboard: EV Table Page  ────────────────────────────────────
import streamlit as st
import pymysql, re
from collections import defaultdict
from datetime import datetime
import pandas as pd

# ───────────────────  PAGE CONFIG  ───────────────────
st.set_page_config(page_title="EV Table", layout="wide")
st.markdown("<h1 style='text-align: center;'>NBA Futures EV Table</h1>", unsafe_allow_html=True)
st.markdown("<h3 style='text-align: center; color: gray;'>among markets tracked in <code>futures_db</code></h3>", unsafe_allow_html=True)

# (DB helper, odds helper, mapping, and SQL logic remain unchanged)
# Replace best_odds_decimal_prob with updated version:
def best_odds_decimal_prob(event_type, event_label, participant, cutoff_dt, fut_conn, vig_map):
    tbl = futures_table_map.get((event_type, event_label))
    if not tbl: return 1.0, 0.0
    alias = team_alias_map.get(participant, participant)
    with with_cursor(fut_conn) as cur:
        cur.execute(
            f"""SELECT {','.join(sportsbook_cols)}
                  FROM {tbl}
                 WHERE team_name = %s AND date_created <= %s
              ORDER BY date_created DESC LIMIT 1""",
            (alias, cutoff_dt)
        )
        row = cur.fetchone()
    if not row: return 1.0, 0.0
    nums = [cast_odds(row.get(c)) for c in sportsbook_cols if row.get(c)]
    nums = [n for n in nums if n]
    if not nums: return 1.0, 0.0
    best = max(nums)
    dec = american_odds_to_decimal(best)
    prob = american_odds_to_prob(best)
    vig = vig_map.get((event_type, event_label), 0.05)
    return dec, prob * (1 - vig)

# Inside ev_table_page()
# Add UI to customize vig per market:
st.markdown("### 🧹 Customize Vig by Market")
vig_inputs = {}
unique_markets = sorted(set((et, el) for et, el in futures_table_map))
with st.expander("Set Vig Percentage Per Market", expanded=False):
    for et, el in unique_markets:
        key = f"{et}|{el}"
        percent = st.slider(
            label=f"{et} — {el}", min_value=0, max_value=20,
            value=5, step=1, key=key
        )
        vig_inputs[(et, el)] = percent / 100.0

# Pass vig_inputs to all best_odds_decimal_prob() calls
# Example:
# dec, p = best_odds_decimal_prob(et, el, pn, now, fut_conn, vig_inputs)

# Everything else stays the same.
