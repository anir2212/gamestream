select
  cast(event_ts as date) as day,
  game_id,
  genre,
  count(*) as purchases,
  sum(amount_cents) as revenue_cents
from {{ ref('stg_events') }}
where event_type = 'purchase' and amount_cents is not null
group by 1, 2, 3
