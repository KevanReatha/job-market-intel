import streamlit as st
import pandas as pd
import boto3
import plotly.express as px
from io import BytesIO

BUCKET = "job-market-intel-kevan"

st.set_page_config(page_title="AI Job Market Intelligence", layout="wide")

st.title("AI / Data Job Market Intelligence")
st.caption("Weekly job market demand based on France Travail API")

s3 = boto3.client("s3")

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

    return pd.concat(frames, ignore_index=True)

@st.cache_data
def load_parquet(key):
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    return pd.read_parquet(BytesIO(obj["Body"].read()))


skill_key = "analytics/skill_demand/v1/dt=2026-03-08/skill_demand_2026-03-08.parquet"
role_key = "analytics/role_demand/v1/dt=2026-03-08/role_demand_2026-03-08.parquet"
role_skill_key = "analytics/role_skill_demand/v1/dt=2026-03-08/role_skill_demand_2026-03-08.parquet"


skills = load_multiple_parquets("analytics/skill_demand/v1/")
roles = load_parquet(role_key)
role_skills = load_multiple_parquets("analytics/role_skill_demand/v1/")

# KPI metrics
col1, col2, col3 = st.columns(3)

col1.metric("Total Skills Detected", len(skills))
col2.metric("Total Roles Detected", len(roles))
col3.metric("Role-Skill Relationships", len(role_skills))


# Sidebar filters
st.sidebar.title("Filters")

selected_role = st.sidebar.selectbox(
    "Select role",
    sorted(role_skills["role_family"].unique())
)


# Charts section
col1, col2 = st.columns(2)

with col1:

    st.subheader("Top Skills")

    skills_sorted = skills.sort_values("demand", ascending=False).head(20)

    fig = px.bar(
        skills_sorted,
        x="skill",
        y="demand",
        title="Top Skills"
    )

    st.plotly_chart(fig, use_container_width=True)


with col2:

    st.subheader("Top Roles")

    roles_sorted = roles.sort_values("demand", ascending=False).head(20)

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
]

filtered = filtered.sort_values("demand", ascending=False).head(10)

fig = px.bar(
    filtered,
    x="skill",
    y="demand",
    title=f"Top Skills for {selected_role}"
)

st.plotly_chart(fig, use_container_width=True)

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

fig = px.line(
    role_trend,
    x="dt",
    y="demand",
    color="skill",
    markers=True,
    title=f"Weekly Skill Demand Trend for {selected_role}"
)

st.plotly_chart(fig, use_container_width=True)