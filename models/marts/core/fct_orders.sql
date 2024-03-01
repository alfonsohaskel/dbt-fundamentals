
with customers as (

    select * from {{ ref('stg_customers') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

payment as (
    select * from {{ ref('stg_payments') }}
),

final as (
    select 
        order_id
    ,   customer_id
    ,   sum(amount) as amount
    from orders
    left join payment using (order_id)
    group by 1, 2

)

select * from final