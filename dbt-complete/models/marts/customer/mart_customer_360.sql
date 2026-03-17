{{
    config(
        materialized='table',
        partition_by={
            "field": "updated_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=["customer_segment", "kyc_status", "risk_rating"],
        description='Customer 360 view — unified profile combining core banking data with transaction behavior and loan exposure.',
        meta={
            "owner": "analytics",
            "domain": "customer",
            "refresh": "daily",
            "sla_hours": 6,
        }
    )
}}

with customers as (
    select * from {{ ref('stg_customers') }}
),

accounts as (
    select * from {{ ref('stg_accounts') }}
),

credit_scores as (
    select * from {{ ref('stg_credit_scores') }}
    where is_latest_score = true
),

loan_applications as (
    select * from {{ ref('stg_loan_applications') }}
),

branches as (
    select * from {{ ref('stg_branches') }}
),

-- Transaction summary per customer (last 30, 90, 365 days)
txn_summary as (
    select
        customer_id,

        -- All-time
        count(*)                                                        as total_transactions,
        sum(case when is_completed then amount_idr else 0 end)          as total_txn_volume_idr,
        sum(case when transaction_category = 'debit' and is_completed then amount_idr else 0 end)
                                                                        as total_debit_idr,
        sum(case when transaction_category = 'credit' and is_completed then amount_idr else 0 end)
                                                                        as total_credit_idr,
        max(transaction_at)                                             as last_transaction_at,
        min(transaction_at)                                             as first_transaction_at,
        count(distinct date(transaction_at))                            as active_transaction_days,

        -- Last 30 days
        countif(transaction_date >= date_sub(current_date(), interval 30 day) and is_completed)
                                                                        as txn_count_30d,
        sum(case when transaction_date >= date_sub(current_date(), interval 30 day) and is_completed
                 then amount_idr else 0 end)                            as txn_volume_30d_idr,

        -- Last 90 days
        countif(transaction_date >= date_sub(current_date(), interval 90 day) and is_completed)
                                                                        as txn_count_90d,
        sum(case when transaction_date >= date_sub(current_date(), interval 90 day) and is_completed
                 then amount_idr else 0 end)                            as txn_volume_90d_idr,

        -- Last 365 days
        countif(transaction_date >= date_sub(current_date(), interval 365 day) and is_completed)
                                                                        as txn_count_365d,
        sum(case when transaction_date >= date_sub(current_date(), interval 365 day) and is_completed
                 then amount_idr else 0 end)                            as txn_volume_365d_idr,

        -- Channel preferences
        countif(channel = 'mobile' and is_completed)                    as mobile_txn_count,
        countif(channel = 'atm' and is_completed)                       as atm_txn_count,
        countif(channel = 'branch' and is_completed)                    as branch_txn_count

    from {{ ref('stg_transactions') }}
    group by customer_id
),

-- Account summary per customer
account_summary as (
    select
        customer_id,
        count(*)                                                        as total_accounts,
        countif(account_status = 'active')                              as active_accounts,
        countif(product_category = 'savings')                           as savings_count,
        countif(product_category = 'checking')                          as checking_count,
        countif(product_category = 'loan')                              as loan_count,
        countif(product_category = 'credit_card')                       as credit_card_count,
        sum(case when account_status = 'active' then balance else 0 end) as total_balance_idr,
        sum(case when product_category = 'loan' then outstanding_balance else 0 end)
                                                                        as total_outstanding_loan_idr,
        max(case when product_category = 'loan' then dpd else 0 end)    as max_dpd,
        min(opened_date)                                                as first_account_opened
    from {{ ref('stg_accounts') }}
    group by customer_id
),

-- Loan application summary
loan_summary as (
    select
        customer_id,
        count(*)                                                        as total_loan_applications,
        countif(application_status = 'approved')                        as approved_loans,
        countif(application_status = 'rejected')                        as rejected_loans,
        countif(application_status = 'disbursed')                       as disbursed_loans,
        sum(case when application_status = 'disbursed' then approved_amount else 0 end)
                                                                        as total_disbursed_idr,
        max(submitted_at)                                               as last_loan_application_at
    from {{ ref('stg_loan_applications') }}
    group by customer_id
),

final as (
    select
        -- ── Identity ───────────────────────────────────────
        c.customer_id,
        c.customer_uuid,
        c.full_name,
        c.national_id_masked,
        c.date_of_birth,
        c.age_years,
        c.gender,
        c.marital_status,
        c.occupation,
        c.income_range,

        -- ── Contact & Location ─────────────────────────────
        c.email,
        c.phone_primary,
        c.address_city,
        c.address_province,

        -- ── Banking Profile ───────────────────────────────
        c.customer_segment,
        c.kyc_status,
        c.kyc_verified_at,
        c.acquisition_channel,
        c.risk_rating,
        c.is_politically_exposed,

        -- Onboarding branch
        b.branch_name                                                   as onboarding_branch_name,
        b.city                                                          as onboarding_branch_city,

        -- ── Account Summary ───────────────────────────────
        coalesce(a.total_accounts, 0)                                   as total_accounts,
        coalesce(a.active_accounts, 0)                                  as active_accounts,
        coalesce(a.savings_count, 0)                                    as savings_accounts,
        coalesce(a.checking_count, 0)                                   as checking_accounts,
        coalesce(a.loan_count, 0)                                       as loan_accounts,
        coalesce(a.credit_card_count, 0)                                as credit_card_accounts,
        coalesce(a.total_balance_idr, 0)                                as total_balance_idr,
        coalesce(a.total_outstanding_loan_idr, 0)                       as total_outstanding_loan_idr,
        coalesce(a.max_dpd, 0)                                          as max_dpd,
        a.first_account_opened,

        -- ── Transaction Behavior ──────────────────────────
        coalesce(t.total_transactions, 0)                               as total_transactions,
        coalesce(t.total_txn_volume_idr, 0)                             as total_txn_volume_idr,
        coalesce(t.txn_count_30d, 0)                                    as txn_count_30d,
        coalesce(t.txn_volume_30d_idr, 0)                               as txn_volume_30d_idr,
        coalesce(t.txn_count_90d, 0)                                    as txn_count_90d,
        coalesce(t.txn_volume_90d_idr, 0)                               as txn_volume_90d_idr,
        coalesce(t.txn_count_365d, 0)                                   as txn_count_365d,
        coalesce(t.txn_volume_365d_idr, 0)                              as txn_volume_365d_idr,
        t.last_transaction_at,
        t.first_transaction_at,
        coalesce(t.active_transaction_days, 0)                          as active_transaction_days,

        -- Preferred channel
        case
            when coalesce(t.mobile_txn_count, 0) >= coalesce(t.atm_txn_count, 0)
             and coalesce(t.mobile_txn_count, 0) >= coalesce(t.branch_txn_count, 0) then 'mobile'
            when coalesce(t.atm_txn_count, 0) >= coalesce(t.branch_txn_count, 0) then 'atm'
            else 'branch'
        end                                                             as preferred_channel,

        -- ── Loan Portfolio ────────────────────────────────
        coalesce(l.total_loan_applications, 0)                          as total_loan_applications,
        coalesce(l.approved_loans, 0)                                   as approved_loans,
        coalesce(l.rejected_loans, 0)                                   as rejected_loans,
        coalesce(l.total_disbursed_idr, 0)                              as total_disbursed_idr,
        l.last_loan_application_at,

        -- ── Credit Score ──────────────────────────────────
        cs.score_value                                                  as latest_credit_score,
        cs.score_band                                                   as credit_score_band,
        cs.score_date                                                   as credit_score_date,

        -- ── Derived KPIs ──────────────────────────────────
        -- Dormancy flag
        date_diff(current_date(), date(t.last_transaction_at), day)     as days_since_last_txn,
        date_diff(current_date(), date(t.last_transaction_at), day) > {{ var('dormant_days_threshold') }}
                                                                        as is_dormant,

        -- Product breadth (cross-sell indicator)
        coalesce(a.savings_count, 0) + coalesce(a.checking_count, 0)
            + coalesce(a.loan_count, 0) + coalesce(a.credit_card_count, 0)
                                                                        as product_count,

        -- NPL flag
        coalesce(a.max_dpd, 0) >= {{ var('npl_dpd_threshold') }}        as is_npl_customer,

        -- Digital adoption score (0–100)
        round(
            safe_divide(coalesce(t.mobile_txn_count, 0),
                        nullif(coalesce(t.total_transactions, 0), 0)) * 100
        , 1)                                                            as digital_adoption_score,

        -- Tenure
        date_diff(current_date(), a.first_account_opened, month)        as tenure_months,

        -- ── Metadata ──────────────────────────────────────
        c.created_at                                                    as customer_since,
        c.updated_at,
        date(c.updated_at)                                              as updated_date,
        {{ audit_columns() }}

    from customers c
    left join branches b        on c.onboarding_branch_id = b.branch_id
    left join account_summary a on c.customer_id = a.customer_id
    left join txn_summary t     on c.customer_id = t.customer_id
    left join loan_summary l    on c.customer_id = l.customer_id
    left join credit_scores cs  on c.customer_id = cs.customer_id
)

select * from final
