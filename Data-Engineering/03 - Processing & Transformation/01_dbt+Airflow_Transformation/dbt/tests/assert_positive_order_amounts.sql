-- Custom test: Ensure all completed orders have positive amounts

select
    order_id,
    order_total,
    order_status
from {{ ref('fct_orders') }}
where is_completed = true
  and order_total <= 0