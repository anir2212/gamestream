with views as (
  select event_date, user_id, min(event_ts) as first_view
  from {{ ref('stg_events') }}
  where event_type = 'page_view'
  group by event_date, user_id
), wishlists as (
  select v.event_date, v.user_id, v.first_view, min(e.event_ts) as first_wishlist
  from views v
  left join {{ ref('stg_events') }} e
    on e.event_date = v.event_date and e.user_id = v.user_id
   and e.event_type = 'add_to_wishlist' and e.event_ts >= v.first_view
  group by v.event_date, v.user_id, v.first_view
), purchases as (
  select w.event_date, w.user_id, w.first_wishlist, min(e.event_ts) as first_purchase
  from wishlists w
  left join {{ ref('stg_events') }} e
    on e.event_date = w.event_date and e.user_id = w.user_id
   and e.event_type = 'purchase' and e.event_ts >= w.first_wishlist
  group by w.event_date, w.user_id, w.first_wishlist
)
select
  w.event_date as day,
  count(*) as viewers,
  count_if(w.first_wishlist is not null) as wishlisters,
  count_if(p.first_purchase is not null) as purchasers
from wishlists w
join purchases p on p.event_date = w.event_date and p.user_id = w.user_id
group by w.event_date
