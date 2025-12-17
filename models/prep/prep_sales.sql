with order_data as (
    select * from {{ ref('staging_orders')}}
)
,order_details_data as(
    select * from {{ref('staging_order_details')}}
)
,products_data as (
    select * from {{ref('staging_products')}}
)
,category_data as (
    select * from {{ref('staging_categories')}}
)
SELECT
    o.order_id,
    o.customer_id,
    o.order_date,
    extract(year from o.order_date) as order_year,
    extract(month from o.order_date) as order_month,
    extract(day from o.order_date) as order_day,
    det.unit_price,
    det.quantity,
    det.discount,
    (p.unit_price * det.quantity) * (1-det.discount) as revenue,
    p.product_id,
    p.product_name,
    p.category_id,
    cat.category_name
FROM order_data as o
left join order_details_data as det
     using(order_id)
left join products_data as p
    using (product_id)
left join categories_data as cat
    using (category_id)