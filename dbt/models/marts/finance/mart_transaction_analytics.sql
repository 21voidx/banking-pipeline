-- mart_transaction_analytics: Daily transaction volume & revenue
{{ config(materialized='table', partition_by={"field": "transaction_date", "data_type": "date"},
          cluster_by=["channel", "transaction_status"]) }}
select
    transaction_date,
    channel,
    transaction_type_code,
    transaction_status,
    count(*)            as transaction_count,
    sum(amount_idr)     as total_volume_idr,
    sum(fee_amount)     as total_fee_idr,
    avg(amount_idr)     as avg_amount_idr
from {{ ref('stg_transactions') }}
group by 1, 2, 3, 4
