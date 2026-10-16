select
  cast(event_ts as date) as day,
  game_id,
  count_if(review_positive) as positive_reviews,
  count_if(not review_positive) as negative_reviews
from {{ ref('stg_events') }}
where event_type = 'review_posted' and review_positive is not null
group by 1, 2
