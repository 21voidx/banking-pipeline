-- {{
--     config(
--         materialized='view',
--         description='Staged customer credit score history. is_latest_score flags the most recent record per customer.'
--     )
-- }}

-- with source as (
--     select * from {{ source('core_banking', 'credit_scores') }}
-- ),

-- scored as (
--     select
--         cast(score_id as int64)                                 as score_id,
--         cast(customer_id as int64)                              as customer_id,
--         cast(score_value as int64)                              as score_value,
--         lower(trim(cast(score_band as string)))                 as score_band,
--         cast(score_date as date)                                as score_date,
--         cast(score_provider as string)                          as score_provider,

--         -- Classify using macro
--         {{ classify_credit_score('cast(score_value as int64)') }} as score_band_derived,

--         -- Latest score flag (window function)
--         row_number() over (
--             partition by cast(customer_id as int64)
--             order by cast(score_date as date) desc
--         ) = 1                                                   as is_latest_score,

--         cast(created_at as timestamp)                           as created_at,
--         cast(updated_at as timestamp)                           as updated_at

--     from source
-- )

-- select * from scored
