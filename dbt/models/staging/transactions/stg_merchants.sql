-- stg_merchants: Follow stg_transactions.sql pattern
{{ config(materialized='view') }}
select * from {{ source('transactions', 'merchants') }}
