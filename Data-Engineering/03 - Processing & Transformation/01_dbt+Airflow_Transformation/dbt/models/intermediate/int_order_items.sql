{{
  config(
    materialized='ephemeral',
    tags=['intermediate']
  )
}}

-- Note: In a real scenario, this would join order_items table
-- For this demo, we'll create synthetic order items based on order totals

with orders as (
    select * from {{ ref('stg_orders') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

-- Synthetic order items (in production, this would be actual line items)
order_items_expanded as (
    select
        o.order_id,
        o.customer_id,
        o.order_date,
        o.order_status,
        o.payment_method,
        o.order_total,
        o.order_subtotal,
        
        -- Assign products based on order value
        case 
            when o.order_subtotal >= 1000 then 101  -- Laptop
            when o.order_subtotal >= 500 then 108   -- Monitor
            when o.order_subtotal >= 300 then 109   -- Standing Desk
            when o.order_subtotal >= 100 then 107   -- Keyboard
            else 102                                 -- Mouse
        end as product_id,
        
        -- Calculate quantity
        case 
            when o.order_subtotal >= 1000 then 1
            when o.order_subtotal >= 500 then 1
            else cast(o.order_subtotal / 50 as int) + 1
        end as quantity
        
    from orders o
    where o.is_completed
),

enriched as (
    select
        oi.order_id,
        oi.customer_id,
        oi.order_date,
        oi.product_id,
        p.product_name,
        p.category,
        p.price_category,
        oi.quantity,
        p.unit_price,
        p.cost_per_unit,
        
        -- Line item calculations
        oi.quantity * p.unit_price as line_item_revenue,
        oi.quantity * p.cost_per_unit as line_item_cost,
        oi.quantity * (p.unit_price - p.cost_per_unit) as line_item_profit,
        
        -- Order context
        oi.order_total,
        oi.payment_method
        
    from order_items_expanded oi
    inner join products p
        on oi.product_id = p.product_id
)

select * from enriched