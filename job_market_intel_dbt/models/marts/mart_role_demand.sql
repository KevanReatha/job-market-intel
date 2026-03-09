select
    role_family,
    count(*) as demand
from {{ ref
('stg_offres_view') }}
where role_family is not null
  and role_family <> 'other'
group by role_family