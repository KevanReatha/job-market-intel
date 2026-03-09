{{ config(materialized='view', schema='MART') }}

select
    value::string as skill,
    count(*) as demand
from {{ ref('stg_offres_view') }},
lateral flatten(input => skills)
group by skill
order by demand desc