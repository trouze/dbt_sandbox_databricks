{{
  config(
    materialized='sproc',
    database='raw',
    schema='eom_core'
  )
}}

{{ insert_eom('CARDMEMBER') }}