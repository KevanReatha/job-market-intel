{{ config(materialized='view', schema='STAGING') }}

select
    "offer_id" as offer_id,
    "title" as title,
    "title_clean" as title_clean,
    "role_family" as role_family,
    "rome_code" as rome_code,
    "rome_label" as rome_label,
    "created_at" as created_at,
    "updated_at" as updated_at,
    "department_or_location" as department_or_location,
    "company_name" as company_name,
    "company_name_clean" as company_name_clean,
    "contract_type" as contract_type,
    "contract_label" as contract_label,
    "salary_label" as salary_label,
    "description" as description,
    "description_clean" as description_clean,
    "skills" as skills,
    "hiring_industry" as hiring_industry,
    "seniority_level" as seniority_level,
    "domain_focus" as domain_focus,
    "search_keyword" as search_keyword,
    "source" as source
from JOB_MARKET_INTEL.STAGING.STG_OFFRES