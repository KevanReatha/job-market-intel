{{ config(materialized='view', schema='MART') }}

select
    role_family,
    seniority_level,
    count(*) as demand
from {{ ref('stg_offres_view') }}
where role_family is not null
  and role_family <> 'other'
  and seniority_level is not null
  and seniority_level <> 'unknown'
group by role_family, seniority_level
order by role_family, demand desc