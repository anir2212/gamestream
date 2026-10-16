-- Each popularity row represents exactly one day and game.
select day, game_id
from {{ ref('game_popularity') }}
group by day, game_id
having count(*) > 1
