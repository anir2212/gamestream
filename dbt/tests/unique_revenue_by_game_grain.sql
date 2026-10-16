-- Each revenue row represents exactly one day, game, and genre.
select day, game_id, genre
from {{ ref('revenue_by_game') }}
group by day, game_id, genre
having count(*) > 1
