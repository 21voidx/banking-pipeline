-- Custom test: Fraud rate (confirmed_fraud / total flagged) should be < 20%
-- Unusually high confirmed rates may indicate model misconfiguration

with fraud_stats as (
    select
        countif(is_confirmed_fraud = true)  as confirmed_count,
        count(*)                            as total_flagged
    from {{ ref('stg_fraud_flags') }}
    where is_reviewed = true
)

select *
from fraud_stats
where safe_divide(confirmed_count, total_flagged) > 0.20
  and total_flagged > 100   -- Only check when sample size is meaningful
