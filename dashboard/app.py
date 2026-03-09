import streamlit as st
import pandas as pd
import snowflake.connector
import plotly.express as px

st.set_page_config(page_title="AI Job Market Intelligence", layout="wide")

st.title("AI / Data Job Market Intelligence")
st.caption("Analytics powered by Snowflake marts")

def get_connection():
    return snowflake.connector.connect(
        account=st.secrets["SNOWFLAKE_ACCOUNT"],
        user=st.secrets["SNOWFLAKE_USER"],
        password=st.secrets["SNOWFLAKE_PASSWORD"],
        warehouse=st.secrets["SNOWFLAKE_WAREHOUSE"],
        database=st.secrets["SNOWFLAKE_DATABASE"],
        schema=st.secrets["SNOWFLAKE_SCHEMA"],
        role=st.secrets["SNOWFLAKE_ROLE"],
    )

@st.cache_data
def run_query(query: str) -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql(query, conn)
    finally:
        conn.close()

# Load marts
role_demand = run_query("""
    select *
    from JOB_MARKET_INTEL.MART.MART_ROLE_DEMAND
""")

skill_demand = run_query("""
    select *
    from JOB_MARKET_INTEL.MART.MART_SKILL_DEMAND
""")

role_skill_demand = run_query("""
    select *
    from JOB_MARKET_INTEL.MART.MART_ROLE_SKILL_DEMAND
""")

role_demand_by_seniority = run_query("""
    select *
    from JOB_MARKET_INTEL.MART.MART_ROLE_DEMAND_BY_SENIORITY
""")

role_demand_by_domain_focus = run_query("""
    select *
    from JOB_MARKET_INTEL.MART.MART_ROLE_DEMAND_BY_DOMAIN_FOCUS
""")

skill_trends_by_month = run_query("""
    select *
    from JOB_MARKET_INTEL.MART.MART_SKILL_TRENDS_BY_MONTH
""")

role_trends_by_month = run_query("""
    select *
    from JOB_MARKET_INTEL.MART.MART_ROLE_TRENDS_BY_MONTH
""")

# KPI
col1, col2, col3 = st.columns(3)
col1.metric("Unique Roles", role_demand["ROLE_FAMILY"].nunique())
col2.metric("Unique Skills", skill_demand["SKILL"].nunique())
col3.metric("Role-Skill Pairs", len(role_skill_demand))

# Sidebar
st.sidebar.title("Filters")
selected_role = st.sidebar.selectbox(
    "Select role",
    sorted(role_demand["ROLE_FAMILY"].dropna().unique())
)

# Top charts
col1, col2 = st.columns(2)

with col1:
    st.subheader("Top Skills")
    top_skills = skill_demand.sort_values("DEMAND", ascending=False).head(20)
    fig = px.bar(top_skills, x="SKILL", y="DEMAND", title="Top Skills")
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Top Roles")
    top_roles = role_demand.sort_values("DEMAND", ascending=False).head(20)
    fig = px.bar(top_roles, x="ROLE_FAMILY", y="DEMAND", title="Top Roles")
    st.plotly_chart(fig, use_container_width=True)

# Top skills by role
st.subheader("Top Skills by Role")
role_skill_filtered = role_skill_demand[
    role_skill_demand["ROLE_FAMILY"] == selected_role
].sort_values("DEMAND", ascending=False).head(10)

fig = px.bar(
    role_skill_filtered,
    x="SKILL",
    y="DEMAND",
    title=f"Top Skills for {selected_role}"
)
st.plotly_chart(fig, use_container_width=True)

# Seniority + domain
col1, col2 = st.columns(2)

with col1:
    st.subheader("Demand by Seniority")
    seniority_filtered = role_demand_by_seniority[
        role_demand_by_seniority["ROLE_FAMILY"] == selected_role
    ].sort_values("DEMAND", ascending=False)

    fig = px.bar(
        seniority_filtered,
        x="SENIORITY_LEVEL",
        y="DEMAND",
        title=f"Demand by Seniority for {selected_role}"
    )
    fig.update_xaxes(
        categoryorder="array",
        categoryarray=[
            "intern_apprentice", "junior", "mid",
            "senior", "lead", "manager", "architect_principal"
        ]
    )
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Demand by Domain Focus")
    domain_filtered = role_demand_by_domain_focus[
        role_demand_by_domain_focus["ROLE_FAMILY"] == selected_role
    ].sort_values("DEMAND", ascending=False)

    fig = px.bar(
        domain_filtered,
        x="DOMAIN_FOCUS",
        y="DEMAND",
        title=f"Demand by Domain Focus for {selected_role}"
    )
    st.plotly_chart(fig, use_container_width=True)

# Skill trends
st.subheader("Skill Trends by Month")
top_role_skills = role_skill_demand[
    role_skill_demand["ROLE_FAMILY"] == selected_role
].sort_values("DEMAND", ascending=False).head(5)["SKILL"].tolist()

skill_trend_filtered = skill_trends_by_month[
    skill_trends_by_month["SKILL"].isin(top_role_skills)
].copy()

skill_trend_filtered["MONTH"] = pd.to_datetime(skill_trend_filtered["MONTH"])

fig = px.line(
    skill_trend_filtered,
    x="MONTH",
    y="DEMAND",
    color="SKILL",
    markers=True,
    title=f"Monthly Skill Trends for {selected_role}"
)
st.plotly_chart(fig, use_container_width=True)

# Role trends
st.subheader("Role Trends by Month")
role_trends_by_month["MONTH"] = pd.to_datetime(role_trends_by_month["MONTH"])

fig = px.line(
    role_trends_by_month,
    x="MONTH",
    y="DEMAND",
    color="ROLE_FAMILY",
    markers=True,
    title="Monthly Role Trends"
)
st.plotly_chart(fig, use_container_width=True)