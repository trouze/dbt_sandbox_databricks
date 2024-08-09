{% snapshot customer_history %}

{{
    config(
      target_database='TROUZEDB',
      target_schema='ODS',
      unique_key='ID',
      strategy='timestamp',
      updated_at='LOAD_DTS',
    )
}}

select * from {{ source('jaffle_shop','raw_customers') }}

{% endsnapshot %}