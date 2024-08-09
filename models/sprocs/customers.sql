{{
  config(
    materialized='sproc'
  )
}}

{{ insert_eom('CUSTOMERS') }}