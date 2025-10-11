-- Ad-hoc analysis: Revenue trends with moving averages

with daily_revenue as (
    select * from {{ ref('daily_revenue') }}
),

with_moving_avg as (
    select
        date_day,
        total_revenue,
        completed_orders,
        avg_order_value,
        
        -- 7-day moving average
        avg(total_revenue) over (
            order by date_day
            rows between 6 preceding and current row
        ) as revenue_7day_ma,
        
        -- 30-day moving average
        avg(total_revenue) over (
            order by date_day
            rows between 29 preceding and current row
        ) as revenue_30day_ma,
        
        -- Month-over-month growth
        lag(total_revenue, 30) over (order by date_day) as revenue_30days_ago,
        
        -- Year-over-year comparison
        lag(total_revenue, 365) over (order by date_day) as revenue_1year_ago
        
    from daily_revenue
),

with_growth_metrics as (
    select
        *,
        
        -- MoM growth rate
        case 
            when revenue_30days_ago > 0 
            then (total_revenue - revenue_30days_ago) / revenue_30days_ago * 100
            else null
        end as mom_growth_rate,
        
        -- YoY growth rate
        case 
            when revenue_1year_ago > 0 
            then (total_revenue - revenue_1year_ago) / revenue_1year_ago * 100
            else null
        end as yoy_growth_rate
        
    from with_moving_avg
)

select * from with_growth_metrics
order by date_day desc