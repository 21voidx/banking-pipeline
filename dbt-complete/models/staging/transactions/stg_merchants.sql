-- {{
--     config(
--         materialized='view',
--         description='Staged merchant reference data from MySQL transaction system.'
--     )
-- }}

-- with source as (
--     select * from {{ source('transactions', 'merchants') }}
-- ),

-- merchant_categories as (
--     select * from {{ source('transactions', 'merchant_categories') }}
-- ),

-- renamed as (
--     select
--         -- Keys
--         cast(m.merchant_id as int64)                            as merchant_id,
--         cast(m.merchant_uuid as string)                         as merchant_uuid,

--         -- Identity
--         cast(m.merchant_name as string)                         as merchant_name,
--         cast(m.merchant_legal_name as string)                   as merchant_legal_name,
--         cast(m.mcc_code as string)                              as mcc_code,

--         -- MCC enrichment
--         cast(mc.category_name as string)                        as category_name,
--         cast(mc.is_high_risk as bool)                           as is_high_risk_mcc,

--         -- Location
--         initcap(cast(m.city as string))                         as city,
--         initcap(cast(m.province as string))                     as province,
--         cast(m.country as string)                               as country,
--         cast(m.is_online as bool)                               as is_online,

--         -- Risk
--         lower(trim(cast(m.risk_level as string)))               as risk_level,
--         cast(m.risk_level as string) in ('elevated', 'high')   as is_elevated_risk,
--         cast(m.is_active as bool)                               as is_active,

--         -- Audit
--         cast(m.created_at as timestamp)                         as created_at,
--         cast(m.updated_at as timestamp)                         as updated_at,
--         cast(m.deleted_at as timestamp)                         as deleted_at

--     from source m
--     left join merchant_categories mc on m.mcc_code = mc.mcc_code
--     where cast(m.deleted_at as timestamp) is null
-- )

-- select * from renamed
