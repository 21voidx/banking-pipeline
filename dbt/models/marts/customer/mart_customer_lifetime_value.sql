-- mart_customer_lifetime_value: CLV segmentation (RFM model)
-- Follow mart_customer_360.sql pattern
{{ config(materialized='table') }}
select
    customer_id,
    txn_count_365d                          as frequency,
    date_diff(current_date(), date(last_transaction_at), day) as recency_days,
    txn_volume_365d_idr                     as monetary_365d
from {{ ref('mart_customer_360') }}
