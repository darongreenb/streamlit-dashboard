#!/usr/bin/env python3

import os
import re
from collections import defaultdict
from datetime import datetime, date

import pandas as pd
import matplotlib.pyplot as plt
from urllib.parse import quote_plus
import pymysql
import sqlalchemy

# ─────────────────────────────────────────────────────────────────────────────
# 1) Database credentials (from environment variables)
# ─────────────────────────────────────────────────────────────────────────────
BET_HOST = os.environ['BET_HOST']
BET_USER = os.environ['BET_USER']
BET_PW   = os.environ['BET_PW']
BET_DB   = os.environ['BET_DB']

FUT_HOST = os.environ['FUT_HOST']
FUT_USER = os.environ['FUT_USER']
FUT_PW   = os.environ['FUT_PW']
FUT_DB   = os.environ['FUT_DB']

# Build SQLAlchemy engines
bet_pw_escaped = quote_plus(BET_PW)
engine_bet = sqlalchemy.create_engine(
    f"mysql+pymysql://{BET_USER}:{bet_pw_escaped}@{BET_HOST}/{BET_DB}"
)
fut_pw_escaped = quote_plus(FUT_PW)
engine_fut = sqlalchemy.create_engine(
    f"mysql+pymysql://{FUT_USER}:{fut_pw_escaped}@{FUT_HOST}/{FUT_DB}"
)

# ─────────────────────────────────────────────────────────────────────────────
# 2) Odds‐conversion & mapping helpers
# ─────────────────────────────────────────────────────────────────────────────
def american_odds_to_decimal(o: int) -> float:
    if o > 0:
        return 1.0 + o/100.0
    if o < 0:
        return 1.0 + 100.0/abs(o)
    return 1.0

def american_odds_to_prob(o: int) -> float:
    if o > 0:
        return 100.0/(o + 100.0)
    if o < 0:
        return abs(o)/(abs(o) + 100.0)
    return 0.0

def cast_odds(v) -> int:
    if v in (None, "", 0):
        return 0
    if isinstance(v, (int, float)):
        return int(v)
    m = re.search(r"[-+]?\d+", str(v))
    return int(m.group()) if m else 0

futures_table_map = {
    ("Championship","NBA Championship"): "NBAChampionship",
    ("Conference Winner","Eastern Conference"): "NBAEasternConference",
    ("Conference Winner","Western Conference"): "NBAWesternConference",
    ("Defensive Player of Year Award","Award"): "NBADefensivePotY",
    ("Division Winner","Atlantic Division"): "NBAAtlantic",
    ("Division Winner","Central Division"):  "NBACentral",
    ("Division Winner","Northwest Division"): "NBANorthwest",
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

def best_odds_decimal_prob(event_type, event_label, participant, cutoff_dt, vig_map):
    """Fetch up to 100 snapshots and pick the longest (min implied prob) non-zero quote."""
    tbl = futures_table_map.get((event_type, event_label))
    if tbl is None:
        return 1.0, 0.0

    alias = team_alias_map.get(participant, participant)
    with engine_fut.connect() as conn:
        rows = conn.execute(
            sqlalchemy.text(f"""
                SELECT {','.join(sportsbook_cols)}
                  FROM {tbl}
                 WHERE team_name = :alias
                   AND date_created <= :dt
              ORDER BY date_created DESC
                 LIMIT 100
            """), {"alias": alias, "dt": cutoff_dt}
        ).mappings().all()

    for r in rows:
        quotes = [cast_odds(r[c]) for c in sportsbook_cols]
        quotes = [q for q in quotes if q]
        if not quotes:
            continue
        best = min(quotes, key=american_odds_to_prob)
        dec  = american_odds_to_decimal(best)
        prob = american_odds_to_prob(best) * (1 - vig_map.get((event_type, event_label), 0.05))
        return dec, prob

    return 1.0, 0.0

# ─────────────────────────────────────────────────────────────────────────────
# 3) Build full EV table (same logic as your Streamlit app)
# ─────────────────────────────────────────────────────────────────────────────
def build_ev_table():
    now    = datetime.utcnow()
    vig    = {k:0.05 for k in futures_table_map}   # flat 5%
    active_stake, active_exp = defaultdict(float), defaultdict(float)
    realized_np = defaultdict(float)

    # --- ACTIVE NBA FUTURES ---
    df_active = pd.read_sql("""
        SELECT b.WagerID, b.PotentialPayout, b.DollarsAtStake,
               l.EventType, l.EventLabel, l.ParticipantName
          FROM bets b JOIN legs l ON b.WagerID=l.WagerID
         WHERE b.WhichBankroll='GreenAleph'
           AND b.WLCA='Active'
           AND l.LeagueName='NBA'
    """, engine_bet)

    gb = df_active.groupby("WagerID", as_index=False)
    bets = {}
    for wid, grp in gb:
        bets[wid] = {
            "pot":   float(grp.PotentialPayout.iloc[0] or 0),
            "stake": float(grp.DollarsAtStake.iloc[0] or 0),
            "legs":  list(grp[["EventType","EventLabel","ParticipantName"]]
                          .itertuples(index=False, name=None))
        }

    for b in bets.values():
        decs, prob = [], 1.0
        for et, el, pn in b["legs"]:
            dec, p = best_odds_decimal_prob(et, el, pn, now, vig)
            if p == 0:
                prob = 0
                break
            decs.append(dec)
            prob *= p
        if prob == 0:
            continue
        expected = b["pot"] * prob
        exc_sum  = sum(d - 1 for d in decs)
        if exc_sum <= 0:
            continue
        for d in decs:
            w = (d - 1) / exc_sum
            active_stake[(et, el)] += w * b["stake"]
            active_exp  [(et, el)] += w * expected

    # --- REALIZED NBA FUTURES ---
    df_real = pd.read_sql("""
        SELECT b.WagerID, b.NetProfit,
               l.EventType, l.EventLabel, l.ParticipantName
          FROM bets b JOIN legs l ON b.WagerID=l.WagerID
         WHERE b.WhichBankroll='GreenAleph'
           AND b.WLCA IN ('Win','Loss','Cashout')
           AND l.LeagueName='NBA'
    """, engine_bet)

    net       = df_real.groupby("WagerID").NetProfit.first().to_dict()
    legs_map  = df_real.groupby("WagerID")[["EventType","EventLabel","ParticipantName"]] \
                      .apply(lambda df: list(df.itertuples(index=False, name=None))) \
                      .to_dict()

    for wid, legs in legs_map.items():
        npv    = float(net.get(wid, 0))
        decs   = [best_odds_decimal_prob(et, el, pn, now, vig)[0]
                  for et, el, pn in legs]
        exc_sum = sum(d - 1 for d in decs)
        if exc_sum <= 0:
            continue
        for d, (et, el, _) in zip(decs, legs):
            realized_np[(et, el)] += npv * ((d - 1) / exc_sum)

    # --- COMPLETED OTHER SPORTS ---
    df_other = pd.read_sql("""
        SELECT b.NetProfit, l.LeagueName, l.EventType, l.EventLabel
          FROM bets b JOIN legs l ON b.WagerID=l.WagerID
         WHERE b.WhichBankroll='GreenAleph'
           AND b.WLCA IN ('Win','Loss','Cashout')
           AND l.LeagueName <> 'NBA'
    """, engine_bet)

    other = df_other.groupby(["LeagueName","EventType","EventLabel"]) \
                    .NetProfit.sum().to_dict()

    # assemble rows
    rec = []
    for (et, el), tbl in futures_table_map.items():
        stk = active_stake.get((et, el), 0)
        exp = active_exp.get((et, el), 0)
        npv = realized_np.get((et, el), 0)
        rec.append({
            "LeagueName":         "NBA",
            "EventType":          et,
            "EventLabel":         el,
            "ActiveDollarsAtStake": round(stk, 2),
            "ActiveExpectedPayout": round(exp, 2),
            "RealizedNetProfit":    round(npv, 2),
            "ExpectedValue":        round(exp - stk + npv, 2)
        })
    for (lg, et, el), npv in other.items():
        rec.append({
            "LeagueName":           lg,
            "EventType":            et,
            "EventLabel":           el,
            "ActiveDollarsAtStake": 0.0,
            "ActiveExpectedPayout": 0.0,
            "RealizedNetProfit":    round(npv, 2),
            "ExpectedValue":        round(npv, 2)
        })

    df = pd.DataFrame(rec)
    df = df.sort_values(["LeagueName","EventType","EventLabel"]).reset_index(drop=True)

    # replace TOTAL row
    total_net = float(pd.read_sql(
        "SELECT SUM(NetProfit) AS s FROM bets WHERE WhichBankroll='GreenAleph'",
        engine_bet
    ).iloc[0, 0] or 0)
    total_row = {
        "LeagueName":           "TOTAL",
        "EventType":            "",
        "EventLabel":           "",
        "ActiveDollarsAtStake": df.ActiveDollarsAtStake.sum(),
        "ActiveExpectedPayout": df.ActiveExpectedPayout.sum(),
        "RealizedNetProfit":    round(total_net, 2),
        "ExpectedValue":        round(df.ActiveExpectedPayout.sum()
                                      - df.ActiveDollarsAtStake.sum()
                                      + total_net, 2)
    }
    df = pd.concat([df, pd.DataFrame([total_row])], ignore_index=True)
    return df

# ─────────────────────────────────────────────────────────────────────────────
# 4) Build, snapshot, load history & plot
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    ev_df = build_ev_table()

    # Extract today's TOTAL EV
    today_ev = float(ev_df.loc[ev_df.LeagueName == "TOTAL", "ExpectedValue"].iloc[0])
    print(f"🗓️  {date.today()} → TOTAL EV = ${today_ev:,.2f}")

    # Upsert into ev_history
    with engine_bet.begin() as conn:
        conn.execute(
            sqlalchemy.text("""
                REPLACE INTO ev_history (snapshot_date, expected_value)
                VALUES (:d, :ev)
            """), {"d": date.today(), "ev": today_ev}
        )
    print("✅  ev_history updated.")

    # Load complete history and plot
    hist = pd.read_sql("""
        SELECT snapshot_date AS date, expected_value AS ev
          FROM ev_history
         ORDER BY snapshot_date
    """, engine_bet, parse_dates=["date"])

    plt.figure(figsize=(10, 5))
    plt.plot(hist.date, hist.ev, marker="o", linewidth=2)
    plt.title("Historical Total Expected Value over Time", fontsize=16)
    plt.xlabel("Date")
    plt.ylabel("Total EV ($)")
    plt.grid(alpha=0.3)
    plt.tight_layout()
    plt.show()
