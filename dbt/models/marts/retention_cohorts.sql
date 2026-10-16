with activity as (
  select distinct user_id, event_date from {{ ref('stg_events') }}
), cohorts as (
  select user_id, min(event_date) as cohort_date from activity group by user_id
)
select
  c.cohort_date,
  count(*) as cohort_users,
  count_if(d1.user_id is not null) as retained_d1,
  count_if(d7.user_id is not null) as retained_d7,
  cast(count_if(d1.user_id is not null) as double) / count(*) as d1_rate,
  cast(count_if(d7.user_id is not null) as double) / count(*) as d7_rate
from cohorts c
left join activity d1
  on d1.user_id = c.user_id and d1.event_date = date_add('day', 1, c.cohort_date)
left join activity d7
  on d7.user_id = c.user_id and d7.event_date = date_add('day', 7, c.cohort_date)
group by c.cohort_date
