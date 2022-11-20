--CTE => common table expression

with

source as (

    select * from {{ source('jaffle', 'jaffle_shop_customers') }}

)

, final as (

    select
        id
        , last_name || ' ' || first_name as name

    from source

)

select * from final