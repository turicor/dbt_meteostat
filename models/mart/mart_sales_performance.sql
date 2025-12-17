with sales_data as (
    select * from {{ ref('prep_sales')}}
)
select
    order_year
    ,order_month
    ,category_name
    ,sum(revenue) as total_revenue
    ,count(distinct order_id) as total_orders
    ,(sum(revenue) / count (distinct order_id)) as average_revenue_order
from sales_data
group by order_year, order_month, category_name