{{
    config(
        materialized='table',
        partition_by={
            "field": "transaction_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=["channel", "transaction_status", "transaction_type_code"],
        description='Daily transaction volume, revenue, and channel analytics. Primary finance mart.',
        meta={
            "owner": "analytics",
            "domain": "finance",
            "refresh": "daily",
            "sla_hours": 4
        }
    )
}}

with transactions as (
    select * from {{ ref('stg_transactions') }}
),

daily_agg as (
    select
        -- Grain: one row per day × channel × transaction_type × status
        transaction_date,
        channel,
        transaction_type_code,
        transaction_type_name,
        transaction_category,
        payment_method_code,
        transaction_status,
        currency,

        -- Volume metrics
        count(*)                                                as transaction_count,
        count(distinct customer_id)                             as unique_customers,
        count(distinct account_id)                              as unique_accounts,

        -- Completed only
        countif(is_completed)                                   as completed_count,
        countif(is_failed)                                      as failed_count,
        countif(is_reversed)                                    as reversed_count,

        -- Amount metrics (IDR)
        sum(case when is_completed then amount_idr else 0 end)  as total_volume_idr,
        sum(case when is_completed then fee_amount else 0 end)  as total_fee_idr,
        sum(case when is_completed then total_amount_idr else 0 end)
                                                                as total_gross_idr,
        avg(case when is_completed then amount_idr end)         as avg_amount_idr,
        min(case when is_completed then amount_idr end)         as min_amount_idr,
        max(case when is_completed then amount_idr end)         as max_amount_idr,

        -- Direction split
        sum(case when transaction_category = 'debit' and is_completed
                 then amount_idr else 0 end)                    as total_debit_idr,
        sum(case when transaction_category = 'credit' and is_completed
                 then amount_idr else 0 end)                    as total_credit_idr,

        -- Hour distribution (peak detection)
        countif(transaction_hour between 6 and 11)              as morning_count,
        countif(transaction_hour between 12 and 17)             as afternoon_count,
        countif(transaction_hour between 18 and 22)             as evening_count,
        countif(transaction_hour between 23 and 23
             or transaction_hour between 0 and 5)               as offhours_count,

        -- Success rate
        safe_divide(
            countif(is_completed),
            count(*)
        )                                                       as success_rate

    from transactions
    group by 1, 2, 3, 4, 5, 6, 7, 8
)

select
    *,
    -- Failure rate (useful for SLA monitoring)
    1 - success_rate                                            as failure_rate,
    -- Revenue per transaction
    safe_divide(total_fee_idr, nullif(completed_count, 0))      as fee_per_txn_idr,
    {{ audit_columns() }}

from daily_agg
