{{
  config(
    materialized='view',
    tags=['staging', 'customers']
  )
}}

with source as (
    select * from {{ ref('raw_customers') }}
),

cleaned as (
    select
        customer_id,
        
        -- Name standardization
        initcap(trim(first_name)) as first_name,
        initcap(trim(last_name)) as last_name,
        lower(trim(email)) as email,
        
        -- Date parsing
        cast(signup_date as date) as signup_date,
        
        -- Standardize country codes
        upper(trim(country)) as country_code,
        
        -- Customer segment standardization
        lower(trim(customer_segment)) as customer_segment,
        
        -- Derived fields
        concat(
            initcap(trim(first_name)), 
            ' ', 
            initcap(trim(last_name))
        ) as full_name,
        
        -- Metadata
        current_timestamp() as dbt_loaded_at,
        '{{ invocation_id }}' as dbt_invocation_id
        
    from source
    
    -- Data quality filters
    where email is not null
      and email like '%@%'
      and signup_date is not null
)

select * from cleaned