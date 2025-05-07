import pymysql, pandas as pd
from datetime import date
import streamlit as st

def conn(section):
    s = st.secrets[section]
    return pymysql.connect(
        host=s.host, user=s.user, password=s.password,
        database=s.database, autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )

def save_snapshot(total_ev):
    with conn("futures_db") as c, c.cursor() as cur:
        cur.execute(
            "REPLACE INTO ev_history (snapshot_date, expected_value) VALUES (%s,%s)",
            (date.today(), total_ev)
        )

def load_history():
    with conn("futures_db") as c:
        return pd.read_sql(
            "SELECT snapshot_date AS date, expected_value AS ev "
            "FROM ev_history ORDER BY date", c, parse_dates=["date"]
        )
