{{
  config(
    materialized='view',
    tags=['staging', 'orders']
  )
}}

with source as (
    select * from {{ ref('raw_orders') }}
),

cleaned as (
    select
        order_id,
        customer_id,
        
        -- Date standardization
        cast(order_date as date) as order_date,
        
        -- Status standardization
        lower(trim(order_status)) as order_status,
        
        -- Payment method mapping using variable
        case 
            when lower(trim(payment_method)) = 'cc' then 'credit_card'
            when lower(trim(payment_method)) = 'db' then 'debit_card'
            when lower(trim(payment_method)) = 'pp' then 'paypal'
            when lower(trim(payment_method)) = 'bc' then 'bitcoin'
            else 'unknown'
        end as payment_method,
        
        -- Financial fields
        cast(order_total as decimal(10,2)) as order_total,
        cast(shipping_cost as decimal(10,2)) as shipping_cost,
        cast(order_total - shipping_cost as decimal(10,2)) as order_subtotal,
        
        -- Derived flags
        case 
            when lower(trim(order_status)) = 'completed' then true 
            else false 
        end as is_completed,
        
        case 
            when lower(trim(order_status)) = 'cancelled' then true 
            else false 
        end as is_cancelled,
        
        case 
            when lower(trim(order_status)) = 'returned' then true 
            else false 
        end as is_returned,
        
        -- Metadata
        current_timestamp() as dbt_loaded_at,
        '{{ invocation_id }}' as dbt_invocation_id
        
    from source
    
    -- Data quality filters
    where order_id is not null
      and customer_id is not null
      and order_date is not null
      and order_total >= 0
)

select * from cleaned