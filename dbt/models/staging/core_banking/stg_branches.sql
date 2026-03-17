{{
    config(
        materialized='view',
        description='Staged branch reference data from PostgreSQL core banking.'
    )
}}

with source as (
    select * from {{ source('core_banking', 'branches') }}
),

renamed as (
    select
        cast(branch_id as int64)                                as branch_id,
        cast(branch_code as string)                             as branch_code,
        cast(branch_name as string)                             as branch_name,
        lower(trim(cast(branch_type as string)))                as branch_type,
        initcap(cast(city as string))                           as city,
        initcap(cast(province as string))                       as province,
        cast(address as string)                                 as address,
        cast(phone as string)                                   as phone,
        cast(is_active as bool)                                 as is_active,
        cast(opened_date as date)                               as opened_date,
        date_diff(current_date(), cast(opened_date as date), year)
                                                                as branch_age_years,
        cast(created_at as timestamp)                           as created_at,
        cast(updated_at as timestamp)                           as updated_at

    from source
    where cast(deleted_at as timestamp) is null
)

select * from renamed
