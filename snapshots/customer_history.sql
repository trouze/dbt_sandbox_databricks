{% snapshot customer_history %}

{{
    config(
      target_database='TROUZE_DB',
      target_schema='JAFFLE_SHOP',
      unique_key='CUSTOMER_ID',
      strategy='timestamp',
      updated_at='UPDATED_ON',
    )
}}

select * from {{ source('jshop','customers') }}

{% endsnapshot %}