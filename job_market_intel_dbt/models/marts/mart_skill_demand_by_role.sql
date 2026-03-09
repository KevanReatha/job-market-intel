{{ config(materialized='view', schema='MART') }}

select
    role_family,
    value::string as skill,
    count(*) as demand
from {{ ref('stg_offres_view') }},
lateral flatten(input => skills)
where role_family is not null
  and role_family <> 'other'
group by role_family, skill
order by role_family, demand desc