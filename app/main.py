import streamlit as st
from app.query import *

# Page config
st.set_page_config(page_title="Coretelecom", layout="wide",
                   menu_items={
        'Get Help': 'mailto:olalekanrasaq1331@gmail.com',
        'Report a bug': "mailto:olalekanrasaq1331@gmail.com",
        'About': "Coretelecom customer complaint dashboard webapp"
    }
)

st.sidebar.header("📡Coretelecom")

sel = st.sidebar.selectbox("Navigation", 
                           ["Overview Dashboard", "Complaints via Call", 
                            "Social Media", "Website Forms"])

if sel == "Overview Dashboard":
    st.subheader("Coretelecom Complaints Overview Dashboard")
    col1, col2 = st.columns(2, gap="medium")
    with col1:
        with st.container(border=True):
            total_complaints = get_total_complaints()
            st.metric("**📢 Total Complaints**", value=f"{total_complaints:,}")

    with col2:
        with st.container(border=True):
            customers = get_customers_withComplaint()
            st.metric("**🧑‍🤝‍🧑 Customers with Complaints**", value=f"{customers:,}")

    _col1, _col2, _col3, _col4  = st.columns(4, gap="small")
    with _col1:
        with st.container(border=True):
            resolved_complaints = get_resolved_complaints()
            st.metric("**📢 Resolved Complaints**", value=f"{resolved_complaints:,}")

    with _col2:
        with st.container(border=True):
            pending_complaints = get_pending_complaints()
            st.metric("**📢 Pending Complaints**", value=f"{pending_complaints:,}")

    with _col3:
        with st.container(border=True):
            blocked_complaints = get_blocked_complaints()
            st.metric("**📢 Blocked Complaints**", value=f"{blocked_complaints:,}")

    with _col4:
        with st.container(border=True):
            backlog_complaints = get_backlog_complaints()
            st.metric("**📢 Backlog Complaints**", value=f"{backlog_complaints:,}")

    df_col1, df_col2 = st.columns(2)
    with df_col1:
        with st.container(border=True):
            df = complaints_per_category()
            st.caption("**📊 Number of Complaints Per Category**")
            st.dataframe(df, hide_index=True)
    
    with df_col2:
        with st.container(border=True):
            df = complaints_per_category_resolution()
            st.caption("**📊 Number of Complaints Per Category & Resolution Status**")
            st.dataframe(df, hide_index=True)

if sel == "Complaints via Call":
    st.subheader("Coretelecom Complaints via Calls")
    col1, col2, col3 = st.columns(3, gap="medium")
    with col1:
        with st.container(border=True):
            total_call_complaints = complaints_via_call()
            st.metric("**📢 Total Call Complaints**", value=f"{total_call_complaints:,}")

    with col2:
        with st.container(border=True):
            avg_daily_calls = average_daily_call()
            st.metric("**📞 Average Daily Calls**", value=f"{avg_daily_calls:,.2f}")

    with col3:
        with st.container(border=True):
            avg_call_duration = average_call_duration()
            st.metric("**📞 Average Call Duration (Minutes)**", value=f"{avg_call_duration}")

    chart_col1, chart_col2 = st.columns(2)
    with chart_col1:
        with st.container(border=True):
            df = average_call_complaintsCategory()
            st.caption("**📊 Average Call Duration by Complaint Category**")
            st.bar_chart(data=df, x='complaint_category', y='average_call_duration', 
                         color="#8bfb9a", horizontal=True)
    
    with chart_col2:
        with st.container(border=True):
            df = calls_per_agentExperience()
            st.caption("**📞 Call Complaints Distribution by Agent Experience**")
            st.bar_chart(data=df, x='experience', y='no_calls', color="#fb177a", horizontal=True)

