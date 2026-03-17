{{
    config(
        materialized='table',
        partition_by={
            "field": "snapshot_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=["dpd_bucket", "product_category", "customer_segment"],
        description='Daily loan portfolio snapshot — NPL analysis, DPD buckets, provision estimates.',
        meta={
            "owner": "risk-analytics",
            "domain": "risk",
            "refresh": "daily",
            "sla_hours": 4
        }
    )
}}

with customers as (
    select
        customer_id,
        customer_segment,
        kyc_status,
        risk_rating,
        income_range,
        address_province,
        is_politically_exposed
    from {{ ref('stg_customers') }}
),

accounts as (
    select * from {{ ref('stg_accounts') }}
    where product_category in ('loan', 'credit_card')
),

loan_apps as (
    select
        customer_id,
        product_code,
        product_name,
        approved_amount,
        tenor_months,
        interest_rate,
        disbursed_at,
        days_to_decision
    from {{ ref('stg_loan_applications') }}
    where is_disbursed = true
),

credit_scores as (
    select customer_id, score_value, score_band, score_date
    from {{ ref('stg_credit_scores') }}
    where is_latest_score = true
),

-- Loan-level portfolio view
loan_portfolio as (
    select
        a.account_id,
        a.account_number,
        a.customer_id,
        a.product_code,
        a.product_name,
        a.product_category,
        a.outstanding_balance,
        a.dpd,
        a.opened_date,
        a.interest_rate,
        a.account_status,

        -- DPD bucket classification
        {{ classify_dpd('a.dpd') }}                             as dpd_bucket,

        -- NPL flag (DPD > threshold from dbt_project.yml vars)
        a.dpd >= {{ var('npl_dpd_threshold') }}                 as is_npl,

        -- Provision rate by DPD bucket (BI regulation simplified)
        case
            when a.dpd = 0                   then 0.01   -- 1%  pass
            when a.dpd between 1  and 30     then 0.05   -- 5%  special mention
            when a.dpd between 31 and 60     then 0.15   -- 15% substandard
            when a.dpd between 61 and 90     then 0.50   -- 50% doubtful
            when a.dpd > 90                  then 1.00   -- 100% loss
        end                                                     as provision_rate,

        -- Estimated provision amount
        a.outstanding_balance * case
            when a.dpd = 0                   then 0.01
            when a.dpd between 1  and 30     then 0.05
            when a.dpd between 31 and 60     then 0.15
            when a.dpd between 61 and 90     then 0.50
            when a.dpd > 90                  then 1.00
        end                                                     as estimated_provision_idr,

        -- Customer enrichment
        c.customer_segment,
        c.risk_rating,
        c.income_range,
        c.address_province,
        c.is_politically_exposed,
        c.kyc_status,

        -- Credit score
        cs.score_value                                          as credit_score,
        cs.score_band                                           as credit_score_band

    from accounts a
    inner join customers c     on a.customer_id = c.customer_id
    left join credit_scores cs on a.customer_id = cs.customer_id
)

select
    current_date()                                              as snapshot_date,
    account_id,
    account_number,
    customer_id,
    customer_segment,
    risk_rating,
    income_range,
    address_province,
    is_politically_exposed,
    kyc_status,
    credit_score,
    credit_score_band,
    product_code,
    product_name,
    product_category,
    account_status,
    opened_date,
    interest_rate,
    outstanding_balance,
    dpd,
    dpd_bucket,
    is_npl,
    provision_rate,
    estimated_provision_idr,

    -- Portfolio-level KPIs (available for dashboard aggregation)
    -- NPL ratio = is_npl outstanding / total outstanding (computed at dashboard layer)
    {{ audit_columns() }}

from loan_portfolio
