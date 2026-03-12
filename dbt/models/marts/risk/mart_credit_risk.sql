-- mart_credit_risk: Loan portfolio NPL analysis
{{ config(materialized='table', partition_by={"field": "snapshot_date", "data_type": "date"}) }}
select
    current_date()  as snapshot_date,
    customer_id,
    max_dpd,
    total_outstanding_loan_idr,
    is_npl_customer
from {{ ref('mart_customer_360') }}
