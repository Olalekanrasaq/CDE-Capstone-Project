import streamlit as st
import psycopg2
import pandas as pd
from sqlalchemy import create_engine, text

# Retrieve credentials from Streamlit secrets
redshift_config = st.secrets["connections"]["redshift_db"]

# Construct the connection string
# The 'redshift+psycopg2' dialect is crucial for Redshift connections
connection_string = (
    f"postgresql+psycopg2://{redshift_config['user']}:{redshift_config['password']}@"
    f"{redshift_config['host']}:{redshift_config['port']}/{redshift_config['database']}"
)

# create database connection
@st.cache_resource
def get_connection(connection_string):
    # Create the SQLAlchemy engine
    engine = create_engine(connection_string)
    return engine.connect()

# query fact tables
@st.cache_data(ttl=3600*12)
def get_customers_withComplaint():
    conn = get_connection(connection_string)
    query = text("SELECT count(distinct customer_email) FROM production.fct_all_complaints;")
    result = conn.execute(query)
    return result.fetchone()[0]

@st.cache_data(ttl=3600*12)
def get_total_complaints():
    conn = get_connection(connection_string)
    query = text("SELECT count(*) FROM production.fct_all_complaints;")
    result = conn.execute(query)
    return result.fetchone()[0]

@st.cache_data(ttl=3600*12)
def get_resolved_complaints():
    conn = get_connection(connection_string)
    query = text("SELECT resolved_count " \
    "FROM production.fct_complaints_agg " \
    "WHERE complaint_date IS NULL and complaint_category IS NULL;")
    result = conn.execute(query)
    return result.fetchone()[0]

@st.cache_data(ttl=3600*12)
def get_pending_complaints():
    conn = get_connection(connection_string)
    query = text("SELECT pending_count " \
    "FROM production.fct_complaints_agg " \
    "WHERE complaint_date IS NULL and complaint_category IS NULL;")
    result = conn.execute(query)
    return result.fetchone()[0]

@st.cache_data(ttl=3600*12)
def get_blocked_complaints():
    conn = get_connection(connection_string)
    query = text("SELECT blocked_count " \
    "FROM production.fct_complaints_agg " \
    "WHERE complaint_date IS NULL and complaint_category IS NULL;")
    result = conn.execute(query)
    return result.fetchone()[0]

@st.cache_data(ttl=3600*12)
def get_backlog_complaints():
    conn = get_connection(connection_string)
    query = text("SELECT backlog_count " \
    "FROM production.fct_complaints_agg " \
    "WHERE complaint_date IS NULL and complaint_category IS NULL;")
    result = conn.execute(query)
    return result.fetchone()[0]

@st.cache_data(ttl=3600*12)
def complaints_per_category():
    conn = get_connection(connection_string)
    query = text('SELECT complaint_category, count(complaint_id) as "No of Complaints" ' \
    'FROM production.fct_all_complaints ' \
    'GROUP BY 1;')
    result = conn.execute(query)
    df = pd.DataFrame(result.fetchall(), columns=result.keys())
    return df

@st.cache_data(ttl=3600*12)
def complaints_per_category_resolution():
    conn = get_connection(connection_string)
    query = text("SELECT complaint_category, " \
    "count(case when resolution_status = 'Resolved' then 1 end) as resolved, " \
    "count(case when resolution_status = 'In-Progress' then 1 end) as pending, " \
    "count(case when resolution_status = 'Blocked' then 1 end) as blocked, " \
    "count(case when resolution_status = 'Backlog' then 1 end) as backlog " \
    "FROM production.fct_all_complaints " \
    "GROUP BY 1;")
    result = conn.execute(query)
    df = pd.DataFrame(result.fetchall(), columns=result.keys())
    return df

@st.cache_data(ttl=3600*12)
def complaints_via_call():
    conn = get_connection(connection_string)
    query = text("SELECT count(*) FROM production.fct_call_logs;")
    result = conn.execute(query)
    return result.fetchone()[0]

@st.cache_data(ttl=3600*12)
def average_daily_call():
    conn = get_connection(connection_string)
    query = text("with avg_calls_daily as (" \
    "SELECT calllogsgenerationdate, count(call_id) as no_call " \
    "FROM production.fct_call_logs " \
    "GROUP BY 1" \
    ")" \
    "SELECT avg(no_call) FROM avg_calls_daily;")
    result = conn.execute(query)
    return result.fetchone()[0]

@st.cache_data(ttl=3600*12)
def average_call_duration():
    conn = get_connection(connection_string)
    query = text("SELECT avg(call_duration_minutes) FROM production.fct_call_logs;")
    result = conn.execute(query)
    return result.fetchone()[0]

@st.cache_data(ttl=3600*12)
def average_call_complaintsCategory():
    conn = get_connection(connection_string)
    query = text("SELECT complaint_category, avg(call_duration_minutes) as average_call_duration " \
    "FROM production.fct_call_logs " \
    "GROUP BY 1 " \
    "ORDER BY 2 DESC;")
    result = conn.execute(query)
    df = pd.DataFrame(result.fetchall(), columns=result.keys())
    return df

@st.cache_data(ttl=3600*12)
def calls_per_agentExperience():
    conn = get_connection(connection_string)
    query = text("SELECT experience, count(call_id) as no_calls " \
    "FROM production.fct_call_logs " \
    "GROUP BY 1 " \
    "ORDER BY 2 DESC;")
    result = conn.execute(query)
    df = pd.DataFrame(result.fetchall(), columns=result.keys())
    return df