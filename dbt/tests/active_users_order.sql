-- Every daily user is also in the seven- and thirty-day trailing windows.
select day, dau, wau, mau
from {{ ref('active_users') }}
where dau < 0 or wau < dau or mau < wau
