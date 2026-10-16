-- Each review row represents exactly one day and game.
select day, game_id
from {{ ref('review_sentiment') }}
group by day, game_id
having count(*) > 1
