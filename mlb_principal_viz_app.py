import streamlit as st
import pandas as pd
import re
from collections import defaultdict
from datetime import datetime, date
from urllib.parse import quote_plus
import sqlalchemy

# ─────────────────────────────────────────────────────────────────────────────
# 1) PAGE CONFIG
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="NBA Futures EV Table", layout="wide")
st.title("🏀 NBA Futures EV Table")
st.caption("among markets tracked in `futuresdata` + settled non-NBA bets")

# ─────────────────────────────────────────────────────────────────────────────
# 2) DATABASE CREDENTIALS & ENGINES
# ─────────────────────────────────────────────────────────────────────────────
BET_HOST =    "betting-db.cp86ssaw6cm7.us-east-1.rds.amazonaws.com"
BET_USER =    "admin"
BET_PW =      "7nRB1i2&A-K>"
BET_DB =      "betting_db"

FUT_HOST =    "greenalephfutures.cnwukek8ge3b.us-east-2.rds.amazonaws.com"
FUT_USER =    "admin"
FUT_PW =      "greenalephadmin"
FUT_DB =      "futuresdata"

bet_pw_esc = quote_plus(BET_PW)
fut_pw_esc = quote_plus(FUT_PW)

engine_bet = sqlalchemy.create_engine(
    f"mysql+pymysql://{BET_USER}:{bet_pw_esc}@{BET_HOST}/{BET_DB}"
)
engine_fut = sqlalchemy.create_engine(
    f"mysql+pymysql://{FUT_USER}:{fut_pw_esc}@{FUT_HOST}/{FUT_DB}"
)

# ─────────────────────────────────────────────────────────────────────────────
# 3) ODDS‐CONVERSION HELPERS & MARKET MAPPINGS
# ─────────────────────────────────────────────────────────────────────────────
def american_odds_to_decimal(o: int) -> float:
    if o > 0:   return 1.0 + o/100.0
    if o < 0:   return 1.0 + 100.0/abs(o)
    return 1.0

def american_odds_to_prob(o: int) -> float:
    if o > 0:   return 100.0/(o + 100.0)
    if o < 0:   return abs(o)/(abs(o) + 100.0)
    return 0.0

def cast_odds(v) -> int:
    if v in (None, "", 0): return 0
    if isinstance(v, (int, float)): return int(v)
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
            """),
            {"alias": alias, "dt": cutoff_dt}
        ).mappings().all()
    for r in rows:
        quotes = [cast_odds(r[c]) for c in sportsbook_cols]
        quotes = [q for q in quotes if q]
        if not quotes:
            continue
        # pick the longest (min implied prob)
        best = min(quotes, key=american_odds_to_prob)
        dec  = american_odds_to_decimal(best)
        prob = american_odds_to_prob(best) * (1 - vig_map.get((event_type, event_label), 0.05))
        return dec, prob
    return 1.0, 0.0

# ─────────────────────────────────────────────────────────────────────────────
# 4) BUILD EV TABLE
# ─────────────────────────────────────────────────────────────────────────────
def build_ev_table():
    now = datetime.utcnow()
    vig = {k: 0.05 for k in futures_table_map}  # flat 5%
    active_stake, active_exp = defaultdict(float), defaultdict(float)
    realized_np = defaultdict(float)

    # --- ACTIVE NBA FUTURES ---
    df_active = pd.read_sql("""
        SELECT b.WagerID, b.PotentialPayout, b.DollarsAtStake,
               l.EventType, l.EventLabel, l.ParticipantName
          FROM bets b
          JOIN legs l ON b.WagerID=l.WagerID
         WHERE b.WhichBankroll='GreenAleph'
           AND b.WLCA='Active'
           AND l.LeagueName='NBA'
    """, engine_bet)

    # group by parlay
    for wid, grp in df_active.groupby("WagerID"):
        pot   = float(grp.PotentialPayout.iloc[0] or 0)
        stake = float(grp.DollarsAtStake.iloc[0] or 0)
        legs  = list(grp[["EventType","EventLabel","ParticipantName"]].itertuples(index=False, name=None))
        decs, prob = [], 1.0
        for et, el, pn in legs:
            dec, p = best_odds_decimal_prob(et, el, pn, now, vig)
            if p == 0:
                prob = 0
                break
            decs.append(dec)
            prob *= p
        if prob == 0:
            continue
        expected = pot * prob
        exc_sum  = sum(d - 1 for d in decs)
        if exc_sum <= 0:
            continue
        for d in decs:
            w = (d - 1) / exc_sum
            active_stake[(et, el)] += w * stake
            active_exp  [(et, el)] += w * expected

    # --- REALIZED NBA FUTURES ---
    df_real = pd.read_sql("""
        SELECT b.WagerID, b.NetProfit,
               l.EventType, l.EventLabel, l.ParticipantName
          FROM bets b
          JOIN legs l ON b.WagerID=l.WagerID
         WHERE b.WhichBankroll='GreenAleph'
           AND b.WLCA IN ('Win','Loss','Cashout')
           AND l.LeagueName='NBA'
    """, engine_bet)

    net_map  = df_real.groupby("WagerID").NetProfit.first().to_dict()
    legs_map = df_real.groupby("WagerID")[["EventType","EventLabel","ParticipantName"]] \
                      .apply(lambda df: list(df.itertuples(index=False, name=None))) \
                      .to_dict()

    for wid, legs in legs_map.items():
        npv = float(net_map.get(wid, 0))
        decs = [best_odds_decimal_prob(et, el, pn, now, vig)[0] for et, el, pn in legs]
        exc_sum = sum(d - 1 for d in decs)
        if exc_sum <= 0:
            continue
        for d, (et, el, _) in zip(decs, legs):
            realized_np[(et, el)] += npv * ((d - 1) / exc_sum)

    # --- COMPLETED OTHER SPORTS ---
    df_other = pd.read_sql("""
        SELECT b.NetProfit, l.LeagueName, l.EventType, l.EventLabel
          FROM bets b
          JOIN legs l ON b.WagerID=l.WagerID
         WHERE b.WhichBankroll='GreenAleph'
           AND b.WLCA IN ('Win','Loss','Cashout')
           AND l.LeagueName <> 'NBA'
    """, engine_bet)

    other_map = df_other.groupby(["LeagueName","EventType","EventLabel"]) \
                        .NetProfit.sum().to_dict()

    # assemble all rows
    rec = []
    for (et, el), tbl in futures_table_map.items():
        stk = active_stake.get((et, el), 0)
        exp = active_exp.get((et, el), 0)
        npv = realized_np.get((et, el), 0)
        rec.append({
            "LeagueName": "NBA",
            "EventType": et,
            "EventLabel": el,
            "ActiveDollarsAtStake": round(stk, 2),
            "ActiveExpectedPayout": round(exp, 2),
            "RealizedNetProfit": round(npv, 2),
            "ExpectedValue": round(exp - stk + npv, 2)
        })
    for (lg, et, el), npv in other_map.items():
        rec.append({
            "LeagueName": lg,
            "EventType": et,
            "EventLabel": el,
            "ActiveDollarsAtStake": 0.0,
            "ActiveExpectedPayout": 0.0,
            "RealizedNetProfit": round(npv, 2),
            "ExpectedValue": round(npv, 2)
        })

    df = pd.DataFrame(rec)
    df = df.sort_values(["LeagueName","EventType","EventLabel"]).reset_index(drop=True)

    # override TOTAL row
    total_net = float(pd.read_sql(
        "SELECT SUM(NetProfit) AS s FROM bets WHERE WhichBankroll='GreenAleph'",
        engine_bet
    ).iloc[0,0] or 0)
    total_row = {
        "LeagueName":         "TOTAL",
        "EventType":          "",
        "EventLabel":         "",
        "ActiveDollarsAtStake":    df.ActiveDollarsAtStake.sum(),
        "ActiveExpectedPayout":    df.ActiveExpectedPayout.sum(),
        "RealizedNetProfit":       round(total_net,2),
        "ExpectedValue":           round(
            df.ActiveExpectedPayout.sum()
          - df.ActiveDollarsAtStake.sum()
          + total_net, 2
        )
    }
    df = pd.concat([df, pd.DataFrame([total_row])], ignore_index=True)
    return df

# ─────────────────────────────────────────────────────────────────────────────
# 5) RENDER
# ─────────────────────────────────────────────────────────────────────────────
df = build_ev_table()

# Show summary (exclude TOTAL row)
metrics_df = df[df.LeagueName != "TOTAL"]
col1, col2, col3, col4 = st.columns(4)
col1.metric("💸 Active Stake",         f"${metrics_df.ActiveDollarsAtStake.sum():,.2f}")
col2.metric("📈 Expected Payout",      f"${metrics_df.ActiveExpectedPayout.sum():,.2f}")
col3.metric("💰 Realized Net Profit",  f"${metrics_df.RealizedNetProfit.sum():,.2f}")
col4.metric("⚡️ Expected Value",      f"${metrics_df.ExpectedValue.sum():,.2f}")

st.markdown("### Market-Level Breakdown")
# show table excluding the TOTAL row (summary is above)
table_df = df[df.LeagueName != "TOTAL"].reset_index(drop=True)
styled = (
    table_df.style
      .format({
          "ActiveDollarsAtStake":"${:,.2f}",
          "ActiveExpectedPayout":"${:,.2f}",
          "RealizedNetProfit":   "${:,.2f}",
          "ExpectedValue":       "${:,.2f}",
      })
      .applymap(
          lambda v: "color:green;font-weight:bold" if isinstance(v,(int,float)) and v>0
                    else "color:red;font-weight:bold" if isinstance(v,(int,float)) and v<0
                    else "",
          subset=["ExpectedValue"]
      )
)
st.dataframe(styled, use_container_width=True, height=700)
