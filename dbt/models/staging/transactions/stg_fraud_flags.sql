-- {{
--     config(
--         materialized='view',
--         description='Staged fraud flags from MySQL. ML-generated signals requiring analyst review.'
--     )
-- }}

-- with source as (
--     select * from {{ source('transactions', 'fraud_flags') }}
-- ),

-- renamed as (
--     select
--         -- Keys
--         cast(flag_id as int64)                                  as flag_id,
--         cast(transaction_id as int64)                           as transaction_id,
--         cast(customer_id as int64)                              as customer_id,

--         -- Flag attributes
--         lower(trim(cast(flag_type as string)))                  as flag_type,
--         lower(trim(cast(severity as string)))                   as severity,
--         cast(confidence_score as numeric)                       as confidence_score,
--         cast(model_version as string)                           as model_version,

--         -- Severity as numeric for scoring
--         case lower(trim(cast(severity as string)))
--             when 'critical' then 4
--             when 'high'     then 3
--             when 'medium'   then 2
--             when 'low'      then 1
--             else 0
--         end                                                     as severity_score,

--         -- High-confidence flag
--         cast(confidence_score as numeric)
--             >= cast({{ var('fraud_high_confidence_threshold') }} as numeric)
--                                                                 as is_high_confidence,

--         -- Review outcome
--         cast(is_confirmed_fraud as bool)                        as is_confirmed_fraud,
--         cast(reviewed_by as string)                             as reviewed_by,
--         cast(reviewed_at as timestamp)                          as reviewed_at,
--         cast(reviewed_at as timestamp) is not null              as is_reviewed,
--         cast(notes as string)                                   as notes,

--         -- Review status classification
--         case
--             when cast(is_confirmed_fraud as bool) = true       then 'confirmed_fraud'
--             when cast(is_confirmed_fraud as bool) = false      then 'false_positive'
--             when cast(reviewed_at as timestamp) is not null    then 'reviewed_inconclusive'
--             else 'pending_review'
--         end                                                     as review_status,

--         -- Audit
--         cast(created_at as timestamp)                           as created_at,
--         cast(updated_at as timestamp)                           as updated_at

--     from source
-- )

-- select * from renamed
