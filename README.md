# AI Job Market Intelligence

AI Job Market Intelligence is a data pipeline and analytics dashboard that tracks demand for technology roles and skills using public job postings from the France Travail API.

The project ingests job offers weekly, transforms them into structured analytics datasets, and surfaces insights through a live dashboard.

---

## Dashboard Preview

![Dashboard](docs/images/dashboard_preview_v1_1.png)
![Dashboard](docs/images/dashboard_preview_v1_2.png)
![Dashboard](docs/images/dashboard_preview_v1_3.png)

---

# Project Goal

The objective of this project is to understand **technology job market demand** by answering questions such as:

- Which roles are most in demand?
- Which technical skills are growing?
- What skills are associated with each role?
- How does demand evolve over time?
- What seniority levels are most demanded?
- Which technical domains are associated with each role?

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
3. Clean and normalize job descriptions  
4. Extract skills from job descriptions  
5. Classify job roles  
6. Enrich offers with inferred attributes
7. Build analytics datasets  
8. Serve insights in a Streamlit dashboard

---

# Data Enrichment (V1.1)

Version 1.1 introduces rule-based NLP enrichment to extract additional insights from job descriptions.

New inferred attributes:

### Seniority Level

- intern_apprentice
- junior
- mid
- senior
- lead
- manager
- architect_principal

### Domain Focus

- data_platform
- bi_reporting
- ml_ai
- genai_llm
- cloud_devops
- data_governance_quality
- embedded_iot

These enrichments allow deeper analysis of the job market beyond simple role classification.

---

# Analytics Datasets

The project now generates **five analytics datasets**.

---

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

## role_demand_by_seniority (V1.1)

Demand distribution by role and seniority level.

Columns:

- `role_family`
- `seniority_level`
- `demand`
- `dt`

---

## role_demand_by_domain_focus (V1.1)

Demand distribution by role and technical domain.

Columns:

- `role_family`
- `domain_focus`
- `demand`
- `dt`

---

# Example Insights

Initial results show:

- Software engineering roles dominate overall demand
- Data analyst roles are strongly associated with BI and reporting tools
- Data engineering roles correlate with SQL, Python, Spark and cloud technologies
- AI engineering roles are strongly linked to GenAI and ML topics
- Entry-level demand is highest for analyst-type roles

---

# Limitations

Current version has several limitations:

- Role classification is rule-based
- Skill extraction relies on keyword matching
- Industry inference is experimental
- Some offers fall into the "other" category
- Historical data coverage is still limited
- Dashboard UX is intentionally simple

---

# Roadmap

## V1 — MVP (Completed)

- Weekly ingestion pipeline
- Raw data storage in S3
- Transformation to parquet
- Skill extraction
- Role classification
- Streamlit dashboard
- Automated GitHub Actions pipeline

---

## V1.1 — Enriched Insights (Completed)

- Seniority level inference
- Domain focus inference
- New analytics datasets
- Extended dashboard insights

---

## V2 — Advanced Analytics

Planned improvements:

- Skill demand growth analysis
- Industry demand insights
- Job market trend detection
- Improved dashboard UX

---

## V3 — Data Platform

Future improvements:

- Snowflake data warehouse
- dbt transformation models
- Semantic data layer

---

## V4 — AI Job Market Copilot

Long-term vision