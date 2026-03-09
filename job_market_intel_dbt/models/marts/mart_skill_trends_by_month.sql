{{ config(materialized='view', schema='MART') }}

select
    date_trunc('month', created_at) as month,
    value::string as skill,
    count(*) as demand
from {{ ref('stg_offres_view') }},
lateral flatten(input => skills)
where created_at is not null
group by month, skill
order by month, demand desc