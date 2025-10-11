{{
  config(
    materialized='ephemeral',
    tags=['intermediate']
  )
}}

with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

customer_order_aggregates as (
    select
        o.customer_id,
        
        -- Order counts by status
        count(distinct o.order_id) as total_orders,
        count(distinct case when o.is_completed then o.order_id end) as completed_orders,
        count(distinct case when o.is_cancelled then o.order_id end) as cancelled_orders,
        count(distinct case when o.is_returned then o.order_id end) as returned_orders,
        
        -- Financial aggregates
        sum(case when o.is_completed then o.order_total else 0 end) as total_revenue,
        sum(case when o.is_completed then o.order_subtotal else 0 end) as total_subtotal,
        sum(case when o.is_completed then o.shipping_cost else 0 end) as total_shipping,
        
        -- Average order values
        avg(case when o.is_completed then o.order_total end) as avg_order_value,
        
        -- Date information
        min(o.order_date) as first_order_date,
        max(o.order_date) as last_order_date,
        
        -- Payment preferences
        {{ dbt_utils.listagg(
            measure='distinct o.payment_method', 
            delimiter_text="', '", 
            order_by_clause='order by o.payment_method'
        ) }} as payment_methods_used,
        
        -- Time-based metrics
        datediff('day', min(o.order_date), max(o.order_date)) as customer_lifetime_days
        
    from orders o
    group by o.customer_id
),

joined as (
    select
        c.customer_id,
        c.full_name,
        c.email,
        c.signup_date,
        c.country_code,
        c.customer_segment,
        
        -- Order metrics
        coalesce(agg.total_orders, 0) as total_orders,
        coalesce(agg.completed_orders, 0) as completed_orders,
        coalesce(agg.cancelled_orders, 0) as cancelled_orders,
        coalesce(agg.returned_orders, 0) as returned_orders,
        
        -- Financial metrics
        coalesce(agg.total_revenue, 0) as lifetime_value,
        coalesce(agg.total_subtotal, 0) as lifetime_subtotal,
        coalesce(agg.total_shipping, 0) as lifetime_shipping,
        coalesce(agg.avg_order_value, 0) as avg_order_value,
        
        -- Dates
        agg.first_order_date,
        agg.last_order_date,
        
        -- Behavioral
        agg.payment_methods_used,
        coalesce(agg.customer_lifetime_days, 0) as customer_lifetime_days,
        
        -- Customer classification
        case 
            when coalesce(agg.total_revenue, 0) >= {{ var('high_value_customer_threshold') }} 
                then 'high_value'
            when coalesce(agg.total_revenue, 0) >= 1000 
                then 'medium_value'
            else 'low_value'
        end as value_segment,
        
        -- Churn risk indicator
        case 
            when agg.last_order_date is null then 'never_ordered'
            when datediff('day', agg.last_order_date, current_date) > {{ var('days_to_churn') }} 
                then 'at_risk'
            when datediff('day', agg.last_order_date, current_date) > 30 
                then 'declining'
            else 'active'
        end as customer_status,
        
        -- Return rate
        case 
            when coalesce(agg.completed_orders, 0) > 0 
            then cast(agg.returned_orders as float) / agg.completed_orders 
            else 0 
        end as return_rate
        
    from customers c
    left join customer_order_aggregates agg
        on c.customer_id = agg.customer_id
)

select * from joined