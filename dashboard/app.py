import streamlit as st
import pandas as pd
import boto3
import plotly.express as px
from io import BytesIO
import os

BUCKET = os.getenv("S3_BUCKET", "job-market-intel-kevan")

st.set_page_config(page_title="AI Job Market Intelligence", layout="wide")

st.title("AI / Data Job Market Intelligence")
st.caption("Weekly job market demand based on France Travail API")

s3 = boto3.client("s3")


@st.cache_data
def load_parquet(key):
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    return pd.read_parquet(BytesIO(obj["Body"].read()))


@st.cache_data
def load_multiple_parquets(prefix):
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)

    frames = []

    for obj in response.get("Contents", []):
        key = obj["Key"]

        if key.endswith(".parquet"):
            data = load_parquet(key)

            dt = key.split("dt=")[1].split("/")[0]
            data["dt"] = dt

            frames.append(data)

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)


# Load analytics tables
skills = load_multiple_parquets("analytics/skill_demand/v1/")
roles = load_multiple_parquets("analytics/role_demand/v1/")
role_skills = load_multiple_parquets("analytics/role_skill_demand/v1/")
role_seniority = load_multiple_parquets("analytics/role_demand_by_seniority/v1/")
role_domain_focus = load_multiple_parquets("analytics/role_demand_by_domain_focus/v1/")

# KPI metrics
col1, col2, col3 = st.columns(3)

col1.metric("Total Skills Detected", len(skills))
col2.metric("Total Roles Detected", len(roles["role_family"].unique()) if not roles.empty else 0)
col3.metric("Role-Skill Relationships", len(role_skills))

# Sidebar filters
st.sidebar.title("Filters")

available_roles = sorted(role_skills["role_family"].dropna().unique())

selected_role = st.sidebar.selectbox(
    "Select role",
    available_roles
)

# Charts section
col1, col2 = st.columns(2)

with col1:
    st.subheader("Top Skills")

    skills_sorted = (
        skills.groupby("skill", as_index=False)["demand"]
        .sum()
        .sort_values("demand", ascending=False)
        .head(20)
    )

    fig = px.bar(
        skills_sorted,
        x="skill",
        y="demand",
        title="Top Skills"
    )

    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Top Roles")

    roles_sorted = (
        roles.groupby("role_family", as_index=False)["demand"]
        .sum()
        .sort_values("demand", ascending=False)
        .head(20)
    )

    fig = px.bar(
        roles_sorted,
        x="role_family",
        y="demand",
        title="Top Roles"
    )

    st.plotly_chart(fig, use_container_width=True)

# Role skill section
st.subheader("Top Skills by Role")

filtered = role_skills[
    role_skills["role_family"] == selected_role
].copy()

filtered = (
    filtered.groupby("skill", as_index=False)["demand"]
    .sum()
    .sort_values("demand", ascending=False)
    .head(10)
)

fig = px.bar(
    filtered,
    x="skill",
    y="demand",
    title=f"Top Skills for {selected_role}"
)

st.plotly_chart(fig, use_container_width=True)

# Skill trend section
st.subheader("Skill Trend Over Time")

role_trend = role_skills[role_skills["role_family"] == selected_role].copy()

top_role_skills = (
    role_trend.groupby("skill")["demand"]
    .sum()
    .sort_values(ascending=False)
    .head(5)
    .index
)

role_trend = role_trend[role_trend["skill"].isin(top_role_skills)].copy()

# Fix time axis ordering
role_trend["dt"] = pd.to_datetime(role_trend["dt"])

fig = px.line(
    role_trend,
    x="dt",
    y="demand",
    color="skill",
    markers=True,
    title=f"Weekly Skill Demand Trend for {selected_role}"
)

st.plotly_chart(fig, use_container_width=True)

# V1.1 section
st.header("V1.1 Enriched Insights")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Demand by Seniority")

    seniority_filtered = role_seniority[
        role_seniority["role_family"] == selected_role
    ].copy()

    seniority_filtered = seniority_filtered.sort_values("demand", ascending=False)

    if seniority_filtered.empty:
        st.info("No seniority data available for this role yet.")
    else:
        fig = px.bar(
            seniority_filtered,
            x="seniority_level",
            y="demand",
            title=f"Demand by Seniority for {selected_role}"
        )

        fig.update_xaxes(
            categoryorder="array",
            categoryarray=["intern_apprentice","junior","mid","senior","lead","manager","architect_principal"]
        )

        st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Demand by Domain Focus")

    domain_filtered = role_domain_focus[
        role_domain_focus["role_family"] == selected_role
    ].copy()

    domain_filtered = domain_filtered.sort_values("demand", ascending=False)

    if domain_filtered.empty:
        st.info("No domain focus data available for this role yet.")
    else:
        fig = px.bar(
            domain_filtered,
            x="domain_focus",
            y="demand",
            title=f"Demand by Domain Focus for {selected_role}"
        )
        st.plotly_chart(fig, use_container_width=True)