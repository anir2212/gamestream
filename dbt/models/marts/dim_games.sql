select game_id, max_by(genre, event_ts) as genre
from {{ ref('stg_events') }}
group by game_id
