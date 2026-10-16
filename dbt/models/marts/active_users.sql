with days as (
  select distinct event_date as day from {{ ref('stg_events') }}
), users_by_day as (
  select distinct event_date, user_id from {{ ref('stg_events') }}
)
select
  d.day,
  count(distinct if(u.event_date = d.day, u.user_id, null)) as dau,
  count(distinct if(u.event_date >= date_add('day', -6, d.day), u.user_id, null)) as wau,
  count(distinct u.user_id) as mau
from days d
left join users_by_day u
  on u.event_date between date_add('day', -29, d.day) and d.day
group by d.day
