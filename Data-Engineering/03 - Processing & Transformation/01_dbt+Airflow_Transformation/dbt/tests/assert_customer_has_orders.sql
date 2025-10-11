-- Custom test: Ensure high-value customers have at least one order

with high_value_customers as (
    select customer_id
    from {{ ref('dim_customers') }}
    where is_high_value = true
),

customer_order_count as (
    select 
        customer_id,
        count(*) as order_count
    from {{ ref('fct_orders') }}
    group by customer_id
)

select 
    hvc.customer_id
from high_value_customers hvc
left join customer_order_count coc
    on hvc.customer_id = coc.customer_id
where coalesce(coc.order_count, 0) = 0