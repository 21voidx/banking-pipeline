-- stg_fraud_flags: Follow stg_transactions.sql pattern
{{ config(materialized='view') }}
select * from {{ source('transactions', 'fraud_flags') }}
