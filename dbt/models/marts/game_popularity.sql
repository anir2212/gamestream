select
  cast(event_ts as date) as day,
  game_id,
  count_if(event_type = 'page_view') as views,
  count_if(event_type = 'play_start') as play_starts,
  count_if(event_type = 'purchase') as purchases,
  count(distinct user_id) as active_users
from {{ ref('stg_events') }}
group by 1, 2
