{{
  config(
    materialized='incremental',
    unique_key='date_day',
    on_schema_change='fail',
    tags=['marts', 'metrics', 'incremental']
  )
}}

with orders as (
    select
        order_date,
        order_total,
        order_subtotal,
        shipping_cost,
        payment_method,
        customer_segment,
        country_code,
        is_completed
    from {{ ref('fct_orders') }}
    
    {% if is_incremental() %}
    -- Only process new dates
    where order_date > (select max(date_day) from {{ this }})
    {% endif %}
),

daily_aggregates as (
    select
        order_date as date_day,
        
        -- Order counts
        count(*) as total_orders,
        count(case when is_completed then 1 end) as completed_orders,
        
        -- Revenue metrics
        sum(case when is_completed then order_total else 0 end) as total_revenue,
        sum(case when is_completed then order_subtotal else 0 end) as total_subtotal,
        sum(case when is_completed then shipping_cost else 0 end) as total_shipping,
        
        -- Average order value
        avg(case when is_completed then order_total end) as avg_order_value,
        
        -- By payment method
        sum(case when is_completed and payment_method = 'credit_card' 
            then order_total else 0 end) as revenue_credit_card,
        sum(case when is_completed and payment_method = 'paypal' 
            then order_total else 0 end) as revenue_paypal,
        sum(case when is_completed and payment_method = 'debit_card' 
            then order_total else 0 end) as revenue_debit_card,
        
        -- By customer segment
        sum(case when is_completed and customer_segment = 'premium' 
            then order_total else 0 end) as revenue_premium,
        sum(case when is_completed and customer_segment = 'enterprise' 
            then order_total else 0 end) as revenue_enterprise,
        sum(case when is_completed and customer_segment = 'standard' 
            then order_total else 0 end) as revenue_standard,
        
        -- By geography
        sum(case when is_completed and country_code = 'USA' 
            then order_total else 0 end) as revenue_usa,
        sum(case when is_completed and country_code = 'UK' 
            then order_total else 0 end) as revenue_uk,
        
        -- Metadata
        current_timestamp() as dbt_updated_at,
        '{{ invocation_id }}' as dbt_invocation_id
        
    from orders
    group by order_date
)

select * from daily_aggregates