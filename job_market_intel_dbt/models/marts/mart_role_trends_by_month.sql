{{ config(materialized='view', schema='MART') }}

select
    date_trunc('month', created_at) as month,
    role_family,
    count(*) as demand
from {{ ref('stg_offres_view') }}
where created_at is not null
  and role_family is not null
  and role_family <> 'other'
group by month, role_family
order by month, demand desc