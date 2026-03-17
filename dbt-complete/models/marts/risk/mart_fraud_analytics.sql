{{
    config(
        materialized='table',
        partition_by={
            "field": "transaction_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=["severity", "flag_type", "is_confirmed_fraud"],
        description='Daily fraud analytics mart — combines transaction details with ML fraud flags for risk monitoring.',
        meta={
            "owner": "risk-analytics",
            "domain": "risk",
            "refresh": "daily",
            "sla_hours": 4,
            "sensitivity": "HIGH — contains fraud investigation data"
        }
    )
}}

with fraud_flags as (
    select * from {{ ref('stg_fraud_flags') }}
),

transactions as (
    select * from {{ ref('stg_transactions') }}
),

customers as (
    select
        customer_id,
        customer_segment,
        kyc_status,
        risk_rating,
        address_province,
        is_politically_exposed
    from {{ ref('stg_customers') }}
),

merchants as (
    select * from {{ ref('stg_merchants') }}
),

final as (
    select
        -- ── Flag Identity ─────────────────────────────────
        ff.flag_id,
        ff.transaction_id,
        ff.customer_id,

        -- ── Transaction Context ───────────────────────────
        t.transaction_date,
        t.transaction_at,
        t.transaction_hour,
        t.transaction_day_of_week,
        t.transaction_type_code,
        t.transaction_type_name,
        t.transaction_category,
        t.payment_method_code,
        t.channel,
        t.amount_idr,
        t.currency,
        t.transaction_status,
        t.merchant_id,
        t.reference_number,

        -- ── Merchant Context ──────────────────────────────
        m.merchant_name,
        m.mcc_code,
        m.category_name                                     as merchant_category,
        m.is_online                                         as is_online_merchant,
        m.risk_level                                        as merchant_risk_level,
        m.is_high_risk_mcc,

        -- ── Fraud Signal ──────────────────────────────────
        ff.flag_type,
        ff.severity,
        ff.confidence_score,
        ff.model_version,
        ff.is_confirmed_fraud,
        ff.reviewed_by,
        ff.reviewed_at,
        ff.notes,
        ff.created_at                                       as flagged_at,

        -- ── Customer Risk Profile ─────────────────────────
        c.customer_segment,
        c.kyc_status,
        c.risk_rating                                       as customer_risk_rating,
        c.address_province,
        c.is_politically_exposed,

        -- ── Derived Risk Scores ───────────────────────────
        -- Combined risk score (weighted)
        round(
            (ff.confidence_score * 0.50)
            + (case ff.severity
                   when 'critical' then 0.40
                   when 'high'     then 0.30
                   when 'medium'   then 0.15
                   when 'low'      then 0.05
                   else 0
               end)
            + (case c.risk_rating
                   when 'high'      then 0.10
                   when 'medium'    then 0.05
                   else 0
               end)
        , 4)                                                as combined_risk_score,

        -- High-risk combination flags
        ff.severity in ('high', 'critical')
            and ff.confidence_score >= {{ var('fraud_high_confidence_threshold') }}
                                                            as is_high_priority_alert,

        -- Off-hours transaction flag (midnight to 5am WIB)
        t.transaction_hour between 0 and 4                 as is_off_hours,

        -- High-value flag (above 99th percentile threshold)
        t.amount_idr > 50000000                            as is_high_value,

        -- Review status
        case
            when ff.is_confirmed_fraud = true  then 'confirmed_fraud'
            when ff.is_confirmed_fraud = false then 'false_positive'
            when ff.reviewed_at is not null    then 'reviewed_inconclusive'
            else 'pending_review'
        end                                                as review_status,

        -- Time to review (SLA monitoring)
        case
            when ff.reviewed_at is not null
            then timestamp_diff(ff.reviewed_at, ff.created_at, hour)
            else null
        end                                                as hours_to_review,

        -- SLA breach (> 24 hours unreviewed for critical)
        ff.reviewed_at is null
            and ff.severity = 'critical'
            and timestamp_diff(current_timestamp(), ff.created_at, hour) > 24
                                                            as is_sla_breached,

        -- ── Metadata ──────────────────────────────────────
        ff.updated_at,
        {{ audit_columns() }}

    from fraud_flags ff
    inner join transactions t  on ff.transaction_id = t.transaction_id
    left join merchants m      on t.merchant_id = m.merchant_id
    left join customers c      on ff.customer_id = c.customer_id
)

select * from final
