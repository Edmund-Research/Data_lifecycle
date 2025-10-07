{{
  config(
    materialized='view',
    tags=['staging', 'products']
  )
}}

with source as (
    select * from {{ ref('raw_products') }}
),

cleaned as (
    select
        product_id,
        
        -- Text standardization
        trim(product_name) as product_name,
        lower(trim(category)) as category,
        trim(supplier) as supplier,
        
        -- Financial fields
        cast(unit_price as decimal(10,2)) as unit_price,
        cast(cost as decimal(10,2)) as cost_per_unit,
        
        -- Derived metrics
        cast(unit_price - cost as decimal(10,2)) as profit_margin_amount,
        cast(
            (unit_price - cost) / nullif(unit_price, 0) * 100 
            as decimal(5,2)
        ) as profit_margin_percentage,
        
        -- Product categorization
        case 
            when unit_price >= 500 then 'high_value'
            when unit_price >= 100 then 'medium_value'
            else 'low_value'
        end as price_category,
        
        -- Metadata
        current_timestamp() as dbt_loaded_at,
        '{{ invocation_id }}' as dbt_invocation_id
        
    from source
    
    where product_id is not null
      and product_name is not null
      and unit_price > 0
)

select * from cleaned