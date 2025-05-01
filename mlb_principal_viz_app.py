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

# ───────────────────  DB HELPERS  ───────────────────
# ... unchanged DB helper functions ...

# ───────────────────  ODDS HELPERS  ───────────────────
# ... unchanged odds conversion functions ...

# ───────────────────  MAPS  ───────────────────
futures_table_map = {
    ("Championship","NBA Championship"): "NBAChampionship",
    ("Conference Winner","Eastern Conference"): "NBAEasternConference",
    ("Conference Winner","Western Conference"): "NBAWesternConference",
    ("Defensive Player of Year Award","Award"): "NBADefensivePotY",
    ("Division Winner","Atlantic Division"): "NBAAtlantic",
    ("Division Winner","Central Division"):  "NBACentral",
    ("Division Winner","Northwest Division"):"NBANorthwest",
    ("Division Winner","Pacific Division"):  "NBAPacific",
    ("Division Winner","Southeast Division"): "NBASoutheast",
    ("Division Winner","Southwest Division"): "NBASouthwest",
    ("Most Improved Player Award","Award"):  "NBAMIP",
    ("Most Valuable Player Award","Award"):  "NBAMVP",
    ("Rookie of Year Award","Award"):        "NBARotY",
    ("Sixth Man of Year Award","Award"):     "NBASixthMotY",
}

team_alias_map = {
    "Philadelphia 76ers":"76ers","Milwaukee Bucks":"Bucks","Chicago Bulls":"Bulls",
    "Cleveland Cavaliers":"Cavaliers","Boston Celtics":"Celtics","Los Angeles Clippers":"Clippers",
    "Memphis Grizzlies":"Grizzlies","Atlanta Hawks":"Hawks","Miami Heat":"Heat",
    "Charlotte Hornets":"Hornets","Utah Jazz":"Jazz","Sacramento Kings":"Kings",
    "New York Knicks":"Knicks","Los Angeles Lakers":"Lakers","Orlando Magic":"Magic",
    "Dallas Mavericks":"Mavericks","Brooklyn Nets":"Nets","Denver Nuggets":"Nuggets",
    "Indiana Pacers":"Pacers","New Orleans Pelicans":"Pelicans","Detroit Pistons":"Pistons",
    "Toronto Raptors":"Raptors","Houston Rockets":"Rockets","San Antonio Spurs":"Spurs",
    "Phoenix Suns":"Suns","Oklahoma City Thunder":"Thunder","Minnesota Timberwolves":"Timberwolves",
    "Portland Trail Blazers":"Trail Blazers","Golden State Warriors":"Warriors","Washington Wizards":"Wizards",
}

sportsbook_cols = ["BetMGM","DraftKings","Caesars","ESPNBet","FanDuel","BallyBet","RiversCasino","Bet365"]

# ───────────────────  VIG UI CONFIG  ───────────────────
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

# ───────────────────  ODDS FUNCTION WITH VIG  ───────────────────
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

# The rest of your EV calculation logic remains unchanged
# Just pass vig_inputs wherever best_odds_decimal_prob() is called
