with source as (
    select * from {{source('jshop','orders')}}
),
renamed as (
    select
        ID as order_id,
        USER_ID as customer_id,
        ORDER_DATE as order_date,
        STATUS as order_status,
        current_timestamp() as last_model_run
    from source
)
select * from renamed
