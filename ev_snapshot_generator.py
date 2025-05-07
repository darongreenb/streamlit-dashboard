#!/usr/bin/env python3
"""
NBA Futures EV Snapshot Generator Script

This script calculates the total Expected Value of the NBA futures betting portfolio
and saves it as a snapshot file. It's designed to be run weekly via a cron job
or other scheduler to build up a history of EV measurements over time.

Usage:
    python ev_snapshot_generator.py [--date YYYY-MM-DD]

Arguments:
    --date: Optional date for historical calculation (default is today)
"""

import argparse
import os
import sys
import json
import traceback
from datetime import datetime, timedelta
from pathlib import Path
import pymysql
import re
from collections import defaultdict

# ─────────────────  CONSTANTS  ─────────────────
# Directory to store EV snapshots - match this with the Streamlit app
DATA_DIR = "ev_snapshots"

# Ensure data directory exists
Path(DATA_DIR).mkdir(exist_ok=True)

# ─────────────────  DB HELPERS  ──────────────────
def new_betting_conn():
    """Create a new connection to the betting database with improved error handling"""
    try:
        conn = pymysql.connect(
            host="betting-db.cp86ssaw6cm7.us-east-1.rds.amazonaws.com",
            user="admin",
            password="7nRB1i2&A-K>",
            database="betting_db",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True,
            connect_timeout=10  # Add timeout to prevent hanging
        )
        return conn
    except pymysql.Error as e:
        error_code = e.args[0]
        error_message = e.args[1] if len(e.args) > 1 else str(e)
        print(f"ERROR: Failed to connect to betting database. Error: {error_code}", file=sys.stderr)
        return None

def new_futures_conn():
    """Create a new connection to the futures database with improved error handling"""
    try:
        conn = pymysql.connect(
            host="greenalephfutures.cnwukek8ge3b.us-east-2.rds.amazonaws.com",
            user="admin",
            password="greenalephadmin",
            database="futuresdata",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True,
            connect_timeout=10
        )
        return conn
    except pymysql.Error as e:
        error_code = e.args[0]
        error_message = e.args[1] if len(e.args) > 1 else str(e)
        print(f"ERROR: Failed to connect to futures database. Error: {error_code}", file=sys.stderr)
        return None

def with_cursor(conn):
    """Create a cursor with error handling"""
    if conn is None:
        return None
    
    try:
        conn.ping(reconnect=True)
        return conn.cursor()
    except Exception as e:
        print(f"ERROR: Error creating cursor: {str(e)}", file=sys.stderr)
        return None

# ────────────────  EV CALCULATION  ────────────────
def calculate_total_ev(as_of_date=None):
    """
    Calculate the total Expected Value for the given date
    Uses code from the original EV Table app with modifications
    for historical calculation
    """
    # Odds helpers
    def american_odds_to_decimal(o): return 1.0 + (o/100) if o > 0 else 1.0 + 100/abs(o) if o else 1.0
    def american_odds_to_prob(o): return 100/(o+100) if o > 0 else abs(o)/(abs(o)+100) if o else 0.0
    def cast_odds(v):
        if v in (None, "", 0): return 0
        if isinstance(v, (int, float)): return int(v)
        m = re.search(r"[-+]?\d+", str(v))
        return int(m.group()) if m else 0
    
    # Maps
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
    
    def best_odds_decimal_prob(event_type, event_label, participant, cutoff_dt, fut_conn, vig_map):
        """Get best odds with error handling"""
        if fut_conn is None:
            return 1.0, 0.0
            
        tbl = futures_table_map.get((event_type, event_label))
        if not tbl: return 1.0, 0.0
        
        alias = team_alias_map.get(participant, participant)
        
        cursor = with_cursor(fut_conn)
        if cursor is None:
            return 1.0, 0.0
            
        try:
            cursor.execute(
                f"""SELECT {','.join(sportsbook_cols)}
                      FROM {tbl}
                     WHERE team_name = %s AND date_created <= %s
                  ORDER BY date_created DESC LIMIT 1""",
                (alias, cutoff_dt)
            )
            row = cursor.fetchone()
        except Exception as e:
            print(f"ERROR: Error querying odds data: {str(e)}", file=sys.stderr)
            return 1.0, 0.0
            
        if not row: return 1.0, 0.0
        nums = [cast_odds(row.get(c)) for c in sportsbook_cols if row.get(c)]
        nums = [n for n in nums if n]
        if not nums: return 1.0, 0.0
        best = max(nums)
        dec = american_odds_to_decimal(best)
        prob = american_odds_to_prob(best)
        vig = vig_map.get((event_type, event_label), 0.05)
        return dec, prob * (1 - vig)

    # Default to current time if no date specified
    cutoff_dt = as_of_date if as_of_date else datetime.utcnow()
    
    # Attempt to connect to databases
    bet_conn = new_betting_conn()
    fut_conn = new_futures_conn()
    
    # Check if either connection failed
    if bet_conn is None or fut_conn is None:
        print("ERROR: Unable to connect to databases. Cannot calculate EV.", file=sys.stderr)
        return None

    # Set default vig of 5% for all markets
    vig_inputs = defaultdict(lambda: 0.05)

    # ------- Active wagers -------
    sql_active = """
        SELECT b.WagerID, b.PotentialPayout, b.DollarsAtStake,
            l.EventType, l.EventLabel, l.ParticipantName, b.PlacedDateTime
        FROM bets b JOIN legs l ON b.WagerID = l.WagerID
        WHERE b.WhichBankroll='GreenAleph' AND b.WLCA='Active' AND l.LeagueName='NBA'
        AND b.PlacedDateTime <= %s
    """
    
    cursor = with_cursor(bet_conn)
    if cursor is None:
        print("ERROR: Failed to create cursor for betting database", file=sys.stderr)
        return None
        
    try:
        cursor.execute(sql_active, (cutoff_dt,))
        rows = cursor.fetchall()
    except Exception as e:
        print(f"ERROR: Error querying active wagers: {str(e)}", file=sys.stderr)
        rows = []

    active_bets = defaultdict(lambda: {"pot":0,"stake":0,"legs":[]})
    for r in rows:
        w = active_bets[r["WagerID"]]
        w["pot"]   = w["pot"]   or float(r["PotentialPayout"] or 0)
        w["stake"] = w["stake"] or float(r["DollarsAtStake"] or 0)
        w["legs"].append((r["EventType"], r["EventLabel"], r["ParticipantName"]))

    active_stake, active_exp = defaultdict(float), defaultdict(float)
    for data in active_bets.values():
        pot, stake, legs = data["pot"], data["stake"], data["legs"]
        decs = []; prob = 1.0
        for et,el,pn in legs:
            dec,p = best_odds_decimal_prob(et,el,pn,cutoff_dt,fut_conn,vig_inputs)
            if p == 0: prob = 0; break
            decs.append((dec,et,el)); prob *= p
        if prob == 0: continue
        expected = pot * prob
        sum_exc  = sum(d-1 for d,_,_ in decs)
        if sum_exc <= 0: continue
        for d,et,el in decs:
            w = (d-1)/sum_exc
            active_stake[(et,el)] += w*stake
            active_exp  [(et,el)] += w*expected

    # ------- Realised net profit -------
    sql_real = """
        SELECT b.WagerID, b.NetProfit,
            l.EventType, l.EventLabel, l.ParticipantName, b.SettledDateTime
        FROM bets b JOIN legs l ON b.WagerID = l.WagerID
        WHERE b.WhichBankroll='GreenAleph'
        AND b.WLCA IN ('Win','Loss','Cashout')
        AND l.LeagueName='NBA'
        AND b.SettledDateTime <= %s
    """
    
    try:
        cursor.execute(sql_real, (cutoff_dt,))
        rows = cursor.fetchall()
    except Exception as e:
        print(f"ERROR: Error querying realized profits: {str(e)}", file=sys.stderr)
        rows = []

    wager_net  = defaultdict(float)
    wager_legs = defaultdict(list)
    for r in rows:
        wager_net [r["WagerID"]] = float(r["NetProfit"] or 0)
        wager_legs[r["WagerID"]].append((r["EventType"],r["EventLabel"],r["ParticipantName"]))

    realized_np = defaultdict(float)
    for wid,legs in wager_legs.items():
        net  = wager_net[wid]
        decs = [(best_odds_decimal_prob(et,el,pn,cutoff_dt,fut_conn,vig_inputs)[0], et, el) for et,el,pn in legs]
        sum_exc = sum(d-1 for d,_,_ in decs)
        if sum_exc <= 0: continue
        for d,et,el in decs:
            realized_np[(et,el)] += net * ((d-1)/sum_exc)

    # Close database connections
    if bet_conn:
        bet_conn.close()
    if fut_conn:
        fut_conn.close()

    # ------- Calculate total EV -------
    total_ev = 0
    keys = set(active_stake)|set(active_exp)|set(realized_np)
    
    for et,el in keys:
        stake = active_stake.get((et,el),0)
        exp   = active_exp.get((et,el),0)
        net   = realized_np.get((et,el),0)
        total_ev += (exp - stake + net)
    
    return round(total_ev, 2)

# ────────────────  SNAPSHOT FUNCTIONS  ────────────────
def save_ev_snapshot(date, ev_value):
    """Save a snapshot of the EV value for a specific date"""
    filename = f"{DATA_DIR}/ev_{date.strftime('%Y-%m-%d')}.json"
    data = {
        "date": date.strftime("%Y-%m-%d"),
        "ev": ev_value,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    with open(filename, 'w') as f:
        json.dump(data, f)
    
    return filename

def snapshot_exists(date):
    """Check if a snapshot already exists for the given date"""
    filename = f"{DATA_DIR}/ev_{date.strftime('%Y-%m-%d')}.json"
    return os.path.exists(filename)

# ────────────────  MAIN FUNCTION  ────────────────
def main():
    """Main function to parse arguments and generate snapshot"""
    parser = argparse.ArgumentParser(description="Generate NBA Futures EV snapshot")
    parser.add_argument('--date', type=str, help="Date for historical calculation (YYYY-MM-DD)")
    args = parser.parse_args()
    
    # Parse the date if provided, otherwise use today
    if args.date:
        try:
            target_date = datetime.strptime(args.date, "%Y-%m-%d")
        except ValueError:
            print(f"ERROR: Invalid date format. Please use YYYY-MM-DD", file=sys.stderr)
            return 1
    else:
        target_date = datetime.now()
    
    # Check if snapshot already exists
    if snapshot_exists(target_date):
        print(f"Snapshot for {target_date.strftime('%Y-%m-%d')} already exists. Use --force to override.")
        return 0
    
    # Calculate EV for the target date
    print(f"Calculating EV for {target_date.strftime('%Y-%m-%d')}...")
    ev = calculate_total_ev(target_date)
    
    if ev is None:
        print("ERROR: Failed to calculate EV", file=sys.stderr)
        return 1
    
    # Save the snapshot
    filename = save_ev_snapshot(target_date, ev)
    print(f"Successfully created EV snapshot: {filename}")
    print(f"EV value: ${ev:,.2f}")
    
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f"ERROR: Unhandled exception: {str(e)}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)
