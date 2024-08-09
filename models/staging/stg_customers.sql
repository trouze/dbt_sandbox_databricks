with source as (
    select *
    from {{ source('jaffle_shop','raw_customers') }}
),
renamed as (
    select
        ID as customer_id,
        NAME as name,
        SIGNUP_DATE as signup_date,
        LOAD_DTS as load_dts
    from source
)
select * from renamed