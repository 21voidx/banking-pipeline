{{
    config(
        materialized='table',
        partition_by={
            "field": "snapshot_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=["clv_segment", "customer_segment"],
        description='Customer Lifetime Value using RFM (Recency, Frequency, Monetary) model. Daily snapshot.',
        meta={
            "owner": "analytics",
            "domain": "customer",
            "refresh": "daily",
            "sla_hours": 6
        }
    )
}}

with base as (
    select
        customer_id,
        customer_uuid,
        customer_segment,
        kyc_status,
        risk_rating,
        income_range,
        address_province,
        acquisition_channel,
        tenure_months,
        is_dormant,
        is_npl_customer,
        digital_adoption_score,
        product_count,
        preferred_channel,

        -- RFM inputs
        coalesce(
            date_diff(current_date(), date(last_transaction_at), day),
            9999
        )                                                       as recency_days,
        txn_count_365d                                          as frequency_365d,
        txn_volume_365d_idr                                     as monetary_365d_idr,

        -- Supporting metrics
        txn_count_30d,
        txn_volume_30d_idr,
        txn_count_90d,
        txn_volume_90d_idr,
        total_balance_idr,
        total_outstanding_loan_idr,
        total_disbursed_idr,
        latest_credit_score,
        credit_score_band,
        total_accounts,
        active_accounts,
        customer_since,
        last_transaction_at

    from {{ ref('mart_customer_360') }}
),

-- RFM scoring (1–5 per dimension using percentile-based quintiles)
rfm_scored as (
    select
        *,

        -- Recency score: lower days = better = higher score
        case
            when recency_days <= 7   then 5
            when recency_days <= 30  then 4
            when recency_days <= 90  then 3
            when recency_days <= 180 then 2
            else 1
        end                                                     as r_score,

        -- Frequency score: more transactions = higher score
        case
            when frequency_365d >= 200 then 5
            when frequency_365d >= 100 then 4
            when frequency_365d >= 36  then 3
            when frequency_365d >= 12  then 2
            else 1
        end                                                     as f_score,

        -- Monetary score: higher spending = higher score (IDR)
        case
            when monetary_365d_idr >= 500_000_000  then 5   -- ≥ 500 juta
            when monetary_365d_idr >= 100_000_000  then 4   -- ≥ 100 juta
            when monetary_365d_idr >= 25_000_000   then 3   -- ≥ 25 juta
            when monetary_365d_idr >= 5_000_000    then 2   -- ≥ 5 juta
            else 1
        end                                                     as m_score

    from base
),

clv_final as (
    select
        *,

        -- Composite RFM score (weighted: M 40%, F 35%, R 25%)
        round(
            (m_score * 0.40) + (f_score * 0.35) + (r_score * 0.25)
        , 2)                                                    as rfm_score,

        -- CLV segment based on RFM
        case
            when (m_score * 0.40) + (f_score * 0.35) + (r_score * 0.25) >= 4.5
                then 'champions'
            when (m_score * 0.40) + (f_score * 0.35) + (r_score * 0.25) >= 3.5
                then 'loyal_customers'
            when (m_score * 0.40) + (f_score * 0.35) + (r_score * 0.25) >= 2.5
                then 'potential_loyalists'
            when (m_score * 0.40) + (f_score * 0.35) + (r_score * 0.25) >= 1.5
                then 'at_risk'
            else 'lost'
        end                                                     as clv_segment,

        -- Estimated simple CLV (annualized monetary * retention factor)
        round(
            monetary_365d_idr
            * case
                when recency_days <= 30  then 1.2
                when recency_days <= 90  then 1.0
                when recency_days <= 180 then 0.7
                else 0.3
              end
        , 0)                                                    as estimated_clv_idr,

        -- Cross-sell opportunity flags
        total_accounts = 1                                      as cross_sell_eligible,
        not is_dormant and product_count < 3                    as upsell_candidate,
        is_dormant and recency_days <= 365                      as winback_candidate

    from rfm_scored
)

select
    customer_id,
    customer_uuid,
    customer_segment,
    kyc_status,
    risk_rating,
    income_range,
    address_province,
    acquisition_channel,
    preferred_channel,
    tenure_months,
    product_count,
    active_accounts,
    is_dormant,
    is_npl_customer,
    digital_adoption_score,
    total_balance_idr,
    total_outstanding_loan_idr,
    total_disbursed_idr,
    latest_credit_score,
    credit_score_band,
    recency_days,
    frequency_365d,
    monetary_365d_idr,
    txn_count_30d,
    txn_volume_30d_idr,
    txn_count_90d,
    txn_volume_90d_idr,
    r_score,
    f_score,
    m_score,
    rfm_score,
    clv_segment,
    estimated_clv_idr,
    cross_sell_eligible,
    upsell_candidate,
    winback_candidate,
    last_transaction_at,
    customer_since,
    current_date()                                              as snapshot_date,
    {{ audit_columns() }}

from clv_final
