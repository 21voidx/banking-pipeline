-- {{
--     config(
--         materialized='view',
--         description='Staged accounts from PostgreSQL core banking. One row per bank account.'
--     )
-- }}

-- with source as (
--     select * from {{ source('core_banking', 'accounts') }}
-- ),

-- product_types as (
--     select * from {{ source('core_banking', 'product_types') }}
-- ),

-- renamed as (
--     select
--         -- Keys
--         cast(a.account_id as int64)                             as account_id,
--         cast(a.account_number as string)                        as account_number,
--         cast(a.customer_id as int64)                            as customer_id,
--         cast(a.branch_id as int64)                              as branch_id,

--         -- Product enrichment
--         cast(a.product_type_id as int64)                        as product_type_id,
--         cast(pt.product_code as string)                         as product_code,
--         cast(pt.product_name as string)                         as product_name,
--         lower(trim(cast(pt.product_category as string)))        as product_category,
--         cast(pt.interest_rate as numeric)                       as interest_rate,

--         -- Financials
--         cast(a.currency as string)                              as currency,
--         cast(a.balance as numeric)                              as balance,
--         cast(a.available_balance as numeric)                    as available_balance,
--         cast(a.balance as numeric)
--             - cast(a.available_balance as numeric)              as blocked_balance,

--         -- Status
--         lower(trim(cast(a.account_status as string)))           as account_status,
--         cast(a.account_status as string) = 'active'             as is_active,
--         cast(a.account_status as string) = 'dormant'            as is_dormant,
--         cast(a.account_status as string) = 'closed'             as is_closed,

--         -- Dates
--         cast(a.opened_date as date)                             as opened_date,
--         cast(a.closed_date as date)                             as closed_date,
--         date_diff(
--             coalesce(cast(a.closed_date as date), current_date()),
--             cast(a.opened_date as date),
--             day
--         )                                                       as account_age_days,

--         -- Loan-specific fields
--         -- outstanding_balance & dpd would come from a loan_servicing table in prod
--         case
--             when lower(cast(pt.product_category as string)) in ('loan', 'credit_card')
--             then cast(a.balance as numeric)
--             else 0
--         end                                                     as outstanding_balance,
--         0                                                       as dpd,

--         -- Audit timestamps
--         cast(a.created_at as timestamp)                         as created_at,
--         cast(a.updated_at as timestamp)                         as updated_at,
--         date(cast(a.updated_at as timestamp))                   as updated_date

--     from source a
--     left join product_types pt on a.product_type_id = pt.product_type_id
--     where cast(a.account_status as string) != 'closed'
--        or cast(a.closed_date as date) >= date_sub(current_date(), interval 365 day)
-- )

-- select * from renamed
