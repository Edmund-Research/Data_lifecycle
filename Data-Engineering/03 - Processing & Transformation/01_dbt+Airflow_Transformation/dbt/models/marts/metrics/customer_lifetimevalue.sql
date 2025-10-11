{{
  config(
    materialized='incremental',
    unique_key='customer_id',
    tags=['marts', 'metrics', 'incremental']
  )
}}

with customer_orders as (
    select
        customer_id,
        order_date,
        order_total,
        is_completed
    from {{ ref('fct_orders') }}
    
    {% if is_incremental() %}
    -- Only process new or updated orders
    where order_date >= (select max(last_calculated_date) from {{ this }})
    {% endif %}
),

ltv_calculations as (
    select
        customer_id,
        
        -- Financial metrics
        sum(case when is_completed then order_total else 0 end) as total_revenue,
        count(distinct case when is_completed then order_date end) as total_completed_orders,
        avg(case when is_completed then order_total end) as avg_order_value,
        
        -- Recency, Frequency, Monetary (RFM)
        max(case when is_completed then order_date end) as last_purchase_date,
        datediff('day', 
            min(case when is_completed then order_date end),
            max(case when is_completed then order_date end)
        ) as customer_lifetime_days,
        
        -- Predicted LTV (simple model based on avg order value and frequency)
        avg(case when is_completed then order_total end) * 
        (count(distinct case when is_completed then order_date end) * 1.5) as predicted_ltv,
        
        -- Calculation metadata
        current_date as last_calculated_date
        
    from customer_orders
    group by customer_id
)

select * from ltv_calculations