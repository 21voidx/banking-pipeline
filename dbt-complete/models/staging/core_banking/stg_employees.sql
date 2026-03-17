-- {{
--     config(
--         materialized='view',
--         description='Staged employee reference data from PostgreSQL core banking.'
--     )
-- }}

-- with source as (
--     select * from {{ source('core_banking', 'employees') }}
-- ),

-- renamed as (
--     select
--         cast(employee_id as int64)                              as employee_id,
--         cast(employee_code as string)                           as employee_code,
--         cast(full_name as string)                               as full_name,
--         lower(trim(cast(email as string)))                      as email,
--         lower(trim(cast(role as string)))                       as role,
--         cast(branch_id as int64)                                as branch_id,
--         cast(hire_date as date)                                 as hire_date,
--         date_diff(current_date(), cast(hire_date as date), month)
--                                                                 as tenure_months,
--         cast(is_active as bool)                                 as is_active,
--         cast(created_at as timestamp)                           as created_at,
--         cast(updated_at as timestamp)                           as updated_at

--     from source
--     where cast(deleted_at as timestamp) is null
-- )

-- select * from renamed
