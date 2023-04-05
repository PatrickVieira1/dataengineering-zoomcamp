{{ config(materialized='view') }}


select * from {{ source('staging', 'gas_prices_brazil')}}
limit 30

