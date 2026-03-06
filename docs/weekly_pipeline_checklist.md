# Weekly Pipeline Checklist — Job Market Intel

This document defines the minimum conditions for a successful weekly pipeline run.

The goal is to ensure that the ingestion, transformation, enrichment, and query layers are functioning correctly before automating the pipeline.

---

# 1. Pipeline Objective

The pipeline extracts job offers from the France Travail API, stores raw data in S3, transforms it into a structured dataset, enriches it with metadata, and exposes it through Athena for analysis.

Weekly runs allow monitoring of:

- job role distribution
- data/AI technology demand
- market trends

---

# 2. Expected Pipeline Steps

Each run should perform the following steps:

1. Call France Travail API
2. Store raw JSONL in S3
3. Transform raw data to stage parquet
4. Enrich the dataset with:
   - title_clean
   - company_name_clean
   - description_clean
   - role_family
   - skills
5. Upload parquet to S3 stage layer
6. Refresh Glue catalog
7. Query data with Athena
8. Print quality checks

---

# 3. Stage Schema

Expected columns:

- offer_id
- title
- title_clean
- role_family
- rome_code
- rome_label
- created_at
- updated_at
- department_or_location
- company_name
- company_name_clean
- contract_type
- contract_label
- salary_label
- description
- description_clean
- skills
- source

Partition column:

- dt

---

# 4. Minimum Quality Checks

The pipeline prints basic quality metrics during execution.

Required metrics:

- total_rows
- title_clean_populated_pct
- relevant_role_family_pct
- company_name_clean_populated_pct

Recommended thresholds:

- total_rows > 0
- title_clean_populated_pct >= 95
- relevant_role_family_pct >= 60

Company name coverage is informational only because the source API often omits company names.

---

# 5. Known Limitations

## Company names
Company name availability depends on the source API and may be missing.

## Role classification
role_family is based on rule-based heuristics, not machine learning.

## Skill extraction
Skills are extracted using dictionary-based regex patterns.

This approach may produce:
- false negatives
- false positives
- partial technology coverage

The dictionary will evolve as more data is collected.

---

# 6. Run Frequency

Initial frequency recommendation:

Weekly.

Reasons:

- lower noise
- easier monitoring
- sufficient for observing trends
- appropriate for early-stage pipeline validation

---

# 7. Pre-Automation Strategy

Before scheduling automated runs:

1. Execute several manual weekly runs
2. Confirm pipeline stability
3. Monitor quality metrics
4. Validate Athena queries

After validation, the pipeline can be scheduled automatically.

---

# 8. Example Athena Sanity Queries

Row count check:
```sql
SELECT COUNT(*)
FROM job_market_intel.v2
WHERE dt = 'YYYY-MM-DD';
```

Role distribution:
```sql
SELECT role_family, COUNT(*)
FROM job_market_intel.v2
WHERE dt = 'YYYY-MM-DD'
GROUP BY role_family
ORDER BY COUNT(*) DESC;
```

Top skills:
```sql
SELECT skill, COUNT(*)
FROM job_market_intel.v2
CROSS JOIN UNNEST(skills) AS t(skill)
WHERE dt = 'YYYY-MM-DD'
GROUP BY skill
ORDER BY COUNT(*) DESC;
```