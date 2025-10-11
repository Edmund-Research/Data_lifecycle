{{
  config(
    materialized='table',
    tags=['marts', 'dimensions']
  )
}}

with customer_base as (
    select * from {{ ref('int_customer_orders') }}
),

final as (
    select
        -- Primary key
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_key,
        customer_id,
        
        -- Customer attributes
        full_name,
        email,
        signup_date,
        country_code,
        customer_segment,
        
        -- Behavioral metrics
        total_orders,
        completed_orders,
        cancelled_orders,
        returned_orders,
        
        -- Financial metrics
        lifetime_value,
        avg_order_value,
        
        -- Dates
        first_order_date,
        last_order_date,
        customer_lifetime_days,
        
        -- Segmentation
        value_segment,
        customer_status,
        payment_methods_used,
        return_rate,
        
        -- Flags
        case when lifetime_value >= {{ var('high_value_customer_threshold') }} 
            then true else false 
        end as is_high_value,
        
        case when customer_status = 'active' 
            then true else false 
        end as is_active,
        
        case when return_rate > 0.2 
            then true else false 
        end as is_high_return_customer,
        
        -- Metadata
        current_timestamp() as dbt_updated_at,
        '{{ invocation_id }}' as dbt_invocation_id
        
    from customer_base
)

select * from final