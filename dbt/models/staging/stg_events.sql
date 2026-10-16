select
  event_id, user_id, game_id, event_type, event_ts, ingest_ts,
  cast(event_ts as date) as event_date,
  schema_version, genre, amount_cents, review_positive, device_type
from {{ source('silver', 'events') }}
where event_type in (
  'session_start', 'page_view', 'search', 'add_to_wishlist',
  'purchase', 'play_start', 'play_end', 'review_posted'
)
