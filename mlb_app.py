import streamlit as st
import mysql.connector
from collections import defaultdict
import pandas as pd
import re

# ----------------------
# 1) DB Connection Setup
# ----------------------
@st.cache_resource

def get_db_connections():
    try:
        st.write("Connecting to Betting DB...")
        betting_conn = mysql.connector.connect(
            host=st.secrets["DB_HOST"],
            user=st.secrets["DB_USER"],
            password=st.secrets["DB_PASSWORD"],
            database=st.secrets["DB_NAME"],
            connection_timeout=10
        )
        st.success("Betting DB connected.")
    except Exception as e:
        st.error("Failed to connect to Betting DB")
        st.exception(e)
        raise

    try:
        st.write("Connecting to Futures DB...")
        fdb = st.secrets["FUTURES_DB"]
        futures_conn = mysql.connector.connect(
            host=fdb["host"],
            user=fdb["user"],
            password=fdb["password"],
            database=fdb["database"],
            connection_timeout=10
        )
        st.success("Futures DB connected.")
    except Exception as e:
        st.error("Failed to connect to Futures DB")
        st.exception(e)
        raise

    return betting_conn, futures_conn

# ---------------------------
# 2) Streamlit App Entry
# ---------------------------
st.title("NBA Futures EV Dashboard")
betting_conn, futures_conn = get_db_connections()

# Continue with the rest of your app below this point.
# For example, a message to confirm it's loaded:
st.info("Connections established successfully. Continue building app logic here.")
