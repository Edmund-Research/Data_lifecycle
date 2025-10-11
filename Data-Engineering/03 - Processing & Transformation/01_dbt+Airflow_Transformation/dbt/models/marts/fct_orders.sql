{{
  config(
    materialized='table',
    tags=['marts', 'facts']
  )
}}

with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select 
        customer_id,
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_key,
        customer_segment,
        country_code
    from {{ ref('dim_customers') }}
),

order_items as (
    select 
        order_id,
        sum(line_item_revenue) as total_line_item_revenue,
        sum(line_item_cost) as total_line_item_cost,
        sum(line_item_profit) as total_line_item_profit,
        count(distinct product_id) as distinct_products
    from {{ ref('int_order_items') }}
    group by order_id
),

final as (
    select
        -- Primary key
        {{ dbt_utils.generate_surrogate_key(['o.order_id']) }} as order_key,
        o.order_id,
        
        -- Foreign keys
        c.customer_key,
        o.customer_id,
        
        -- Date information
        o.order_date,
        extract(year from o.order_date) as order_year,
        extract(month from o.order_date) as order_month,
        extract(day from o.order_date) as order_day,
        extract(dayofweek from o.order_date) as order_day_of_week,
        
        -- Order attributes
        o.order_status,
        o.payment_method,
        c.customer_segment,
        c.country_code,
        
        -- Financial metrics
        o.order_total,
        o.order_subtotal,
        o.shipping_cost,
        coalesce(oi.total_line_item_revenue, 0) as line_item_revenue,
        coalesce(oi.total_line_item_cost, 0) as line_item_cost,
        coalesce(oi.total_line_item_profit, 0) as line_item_profit,
        
        -- Derived metrics
        case when o.order_subtotal > 0 
            then o.shipping_cost / o.order_subtotal 
            else 0 
        end as shipping_to_subtotal_ratio,
        
        coalesce(oi.distinct_products, 0) as distinct_products_count,
        
        -- Order flags
        o.is_completed,
        o.is_cancelled,
        o.is_returned,
        
        -- Metadata
        current_timestamp() as dbt_updated_at,
        '{{ invocation_id }}' as dbt_invocation_id
        
    from orders o
    left join customers c
        on o.customer_id = c.customer_id
    left join order_items oi
        on o.order_id = oi.order_id
)

select * from final