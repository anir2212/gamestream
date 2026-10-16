-- Later funnel stages cannot contain more users than earlier stages.
select day, viewers, wishlisters, purchasers
from {{ ref('purchase_funnel') }}
where viewers < 0
   or wishlisters < 0
   or purchasers < 0
   or wishlisters > viewers
   or purchasers > wishlisters
