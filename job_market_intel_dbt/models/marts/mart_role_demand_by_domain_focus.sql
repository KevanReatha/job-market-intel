{{ config(materialized='view', schema='MART') }}

select
    role_family,
    domain_focus,
    count(*) as demand
from {{ ref('stg_offres_view') }}
where role_family is not null
  and role_family <> 'other'
  and domain_focus is not null
  and domain_focus <> 'unknown'
group by role_family, domain_focus
order by role_family, demand desc