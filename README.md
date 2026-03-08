# AI Job Market Intelligence

AI Job Market Intelligence is a data pipeline and analytics dashboard that tracks demand for technology roles and skills using public job postings from the France Travail API.

The project ingests job offers weekly, transforms them into structured analytics datasets, and surfaces insights through a live dashboard.

---

## Dashboard Preview

![Dashboard](docs/images/dashboard_preview_v1_1.png)
![Dashboard](docs/images/dashboard_preview_v1_2.png)
![Dashboard](docs/images/dashboard_preview_v1_3.png)

# Project Goal

The objective of this project is to understand **technology job market demand** by answering questions such as:

- Which roles are most in demand?
- Which technical skills are growing?
- What skills are associated with each role?
- How does demand evolve over time?

This project also demonstrates an **end-to-end data engineering pipeline**.

---

# Architecture

Pipeline overview:
France Travail API
↓
GitHub Actions (weekly ingestion)
↓
S3 raw JSONL storage
↓
Python transformations
↓
Analytics parquet datasets
↓
Streamlit dashboard

---

# Tech Stack

### Data Ingestion
- Python
- France Travail API

### Storage
- AWS S3

### Data Processing
- pandas
- pyarrow

### Orchestration
- GitHub Actions

### Visualization
- Streamlit
- Plotly

---

# Data Pipeline

The weekly pipeline performs the following steps:

1. Fetch job offers from France Travail API  
2. Store raw data in JSONL format in S3  
3. Transform data into parquet datasets  
4. Extract skills and role classifications  
5. Build analytics datasets  
6. Serve insights in a Streamlit dashboard

---

# Analytics Datasets

The project generates three core datasets.

## skill_demand

Aggregated demand for each technical skill.

Columns:

- `skill`
- `demand`
- `dt`

---

## role_demand

Demand aggregated by role family.

Columns:

- `role_family`
- `demand`
- `dt`

---

## role_skill_demand

Mapping between roles and their most demanded skills.

Columns:

- `role_family`
- `skill`
- `demand`
- `dt`

---

# Example Insights

Initial results show:

- Software engineering dominates job demand
- Top skills include SQL, Python, Java, Git and cloud technologies
- Data engineering roles strongly correlate with SQL, Python, Spark and cloud platforms

---

# Limitations (V1)

Current version has several limitations:

- Role classification is rule-based
- Some offers fall into the "other" category
- Skill extraction relies on keyword matching
- Historical data is still limited
- Dashboard UI is minimal

---

# Roadmap

## V1 — Working MVP

Completed features:

- Weekly ingestion pipeline
- Raw data storage in S3
- Transformation to parquet
- Skill extraction
- Role classification
- Streamlit dashboard
- Automated GitHub Actions pipeline

---

## V2 — Industrialized Data Platform

Planned improvements:

- Snowflake data warehouse
- dbt transformation models
- Improved semantic data model
- Better dashboard UX
- Time-series analytics

---

## V3 — Machine Learning

Future features:

- Skill demand forecasting
- Role demand forecasting
- Trend detection

---

## V4 — AI Copilot

Long-term vision:

- Natural language job market exploration
- Skill gap recommendations
- Career path insights

