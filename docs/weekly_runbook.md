# Weekly Runbook — Job Market Intel

This runbook defines the weekly manual operating procedure for the France Travail job market pipeline.

The objective is to run the pipeline in a controlled way before moving to full automation.

---

## 1. Scope

This weekly run covers:

- raw ingestion from France Travail API
- stage transformation to parquet
- enrichment:
  - title_clean
  - company_name_clean
  - description_clean
  - role_family
  - skills
- quality checks
- Athena sanity checks

---

## 2. Weekly Execution Order

### Step 1 — Run raw ingestion

Execute the ingestion script to collect job offers and upload JSONL to S3 raw.

Expected result:

- a new JSONL file exists in:
  - `raw/francetravail/offres/v2/dt=YYYY-MM-DD/`

Success criteria:

- API call succeeds
- row count > 0
- S3 upload succeeds

---

### Step 2 — Run stage transformation

Execute the stage transformation script.

Expected result:

- a new parquet file exists in:
  - `stage/francetravail/offres/v2/dt=YYYY-MM-DD/`

The dataset must include:

- cleaned columns
- role classification
- skills extraction
- quality checks in logs

Success criteria:

- parquet write succeeds
- S3 upload succeeds
- no Python error

---

### Step 3 — Review quality checks

The script must print:

- `total_rows`
- `title_clean_populated_pct`
- `relevant_role_family_pct`
- `company_name_clean_populated_pct`

Minimum weekly acceptance thresholds:

- `total_rows > 0`
- `title_clean_populated_pct >= 95`
- `relevant_role_family_pct >= 60`

`company_name_clean_populated_pct` is monitored but not blocking.

---

### Step 4 — Refresh Glue catalog only if schema changed

Glue crawler must be re-run only when the parquet schema changes.

Examples:
- new column added
- column removed
- datatype changed

If only the values changed but schema stayed the same, crawler refresh is not required.

---

### Step 5 — Run Athena sanity queries

Run the following sanity checks in Athena.

#### Row count

```sql
SELECT COUNT(*)
FROM job_market_intel.v2
WHERE dt = 'YYYY-MM-DD';
```

#### Role distribution

```SQL 
SELECT role_family, COUNT(*) AS nb_offers
FROM job_market_intel.v2
WHERE dt = 'YYYY-MM-DD'
GROUP BY role_family
ORDER BY nb_offers DESC;
```

#### Top skills
```SQL
SELECT skill, COUNT(*) AS nb_offers
FROM job_market_intel.v2
CROSS JOIN UNNEST(skills) AS t(skill)
WHERE dt = 'YYYY-MM-DD'
GROUP BY skill
ORDER BY nb_offers DESC;
```
Success criteria:
	•	Athena query succeeds
	•	counts are coherent
	•	no obvious schema issue
	•	no empty skill output


## 3. Known Limitations

Company names

Company names are often missing from the source API.

Role classification

role_family is heuristic and rule-based.

Skill extraction

skills is dictionary-driven and regex-based.

Coverage will improve over time.

⸻

## 4. Failure Handling

Case 1 — No rows produced

Stop the run.
Investigate:
	•	API access
	•	credentials
	•	source availability
	•	filters

Case 2 — Quality metrics drop unexpectedly

Compare with previous run.
Investigate:
	•	source payload changes
	•	cleaning logic
	•	classification rules

Case 3 — Athena query fails

Check:
	•	Glue table exists
	•	partition path exists in S3
	•	schema has not changed without crawler refresh

⸻

## 5. Weekly Run Status

A weekly run is considered successful when:
	•	raw ingestion succeeded
	•	stage parquet was produced
	•	quality checks passed minimum thresholds
	•	Athena sanity queries succeeded
	•	results are coherent enough for analysis

⸻

## 6. Automation Readiness

The pipeline can move to scheduled weekly automation when:
	•	several manual runs are stable
	•	quality metrics remain acceptable
	•	schema changes are controlled
	•	Athena checks are consistently successful


