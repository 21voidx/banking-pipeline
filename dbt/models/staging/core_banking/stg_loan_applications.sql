-- {{
--     config(
--         materialized='view',
--         description='Staged loan applications from PostgreSQL. Tracks full lifecycle from submission to disbursement.'
--     )
-- }}

-- with source as (
--     select * from {{ source('core_banking', 'loan_applications') }}
-- ),

-- product_types as (
--     select * from {{ source('core_banking', 'product_types') }}
-- ),

-- renamed as (
--     select
--         -- Keys
--         cast(la.application_id as int64)                        as application_id,
--         cast(la.application_number as string)                   as application_number,
--         cast(la.customer_id as int64)                           as customer_id,
--         cast(la.branch_id as int64)                             as branch_id,
--         cast(la.handled_by as int64)                            as handled_by_employee_id,
--         cast(la.product_type_id as int64)                       as product_type_id,

--         -- Product context
--         cast(pt.product_code as string)                         as product_code,
--         cast(pt.product_name as string)                         as product_name,

--         -- Loan financials
--         cast(la.requested_amount as numeric)                    as requested_amount,
--         cast(la.approved_amount as numeric)                     as approved_amount,
--         safe_divide(
--             cast(la.approved_amount as numeric),
--             nullif(cast(la.requested_amount as numeric), 0)
--         )                                                       as approval_ratio,
--         cast(la.tenor_months as int64)                          as tenor_months,
--         cast(la.interest_rate as numeric)                       as interest_rate,
--         cast(la.purpose as string)                              as purpose,

--         -- Collateral
--         lower(trim(cast(la.collateral_type as string)))         as collateral_type,
--         cast(la.collateral_value as numeric)                    as collateral_value,
--         cast(la.collateral_type as string) != 'none'            as has_collateral,

--         -- Status
--         lower(trim(cast(la.application_status as string)))      as application_status,
--         cast(la.application_status as string) in ('approved', 'disbursed')
--                                                                 as is_approved,
--         cast(la.application_status as string) = 'disbursed'    as is_disbursed,
--         cast(la.application_status as string) = 'rejected'     as is_rejected,
--         cast(la.rejection_reason as string)                     as rejection_reason,

--         -- Lifecycle timestamps
--         cast(la.submitted_at as timestamp)                      as submitted_at,
--         cast(la.reviewed_at as timestamp)                       as reviewed_at,
--         cast(la.decided_at as timestamp)                        as decided_at,
--         cast(la.disbursed_at as timestamp)                      as disbursed_at,
--         date(cast(la.submitted_at as timestamp))                as submitted_date,

--         -- Processing SLA (days)
--         case
--             when la.decided_at is not null
--             then date_diff(
--                 date(cast(la.decided_at as timestamp)),
--                 date(cast(la.submitted_at as timestamp)),
--                 day
--             )
--         end                                                     as days_to_decision,

--         -- Audit
--         cast(la.created_at as timestamp)                        as created_at,
--         cast(la.updated_at as timestamp)                        as updated_at

--     from source la
--     left join product_types pt on la.product_type_id = pt.product_type_id
--     where cast(la.deleted_at as timestamp) is null
-- )

-- select * from renamed
