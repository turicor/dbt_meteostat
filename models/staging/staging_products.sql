with source_data as (
    select *
    from {{ source ('northwind', 'products')}}
)
select
    product_id
    ,product_name
    ,supplier_id
    ,category_id
    ,quantity_per_unit
    ,unit_price::numeric as price
from source_data