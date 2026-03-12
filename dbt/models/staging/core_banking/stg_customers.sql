{{
    config(
        materialized='view',
        description='Staged customers from PostgreSQL core banking. PII fields masked for non-PII roles.'
    )
}}

with source as (
    select * from {{ source('core_banking', 'customers') }}
),

renamed as (
    select
        -- Keys
        cast(customer_id as int64)          as customer_id,
        cast(customer_uuid as string)       as customer_uuid,

        -- PII — masked in this layer; raw values available only via BQ column-level access policy
        {{ mask_pii('full_name') }}          as full_name,
        {{ mask_pii('national_id') }}        as national_id_masked,
        -- Non-masked for downstream enrichment (secured via BQ row/column policies)
        cast(national_id as string)         as national_id,
        cast(date_of_birth as date)         as date_of_birth,
        date_diff(current_date(), cast(date_of_birth as date), year)
                                            as age_years,

        -- Demographics
        upper(trim(cast(gender as string)))              as gender,
        lower(trim(cast(marital_status as string)))      as marital_status,
        cast(occupation as string)                       as occupation,
        cast(income_range as string)                     as income_range,

        -- Contact
        lower(trim(cast(email as string)))               as email,
        cast(phone_primary as string)                    as phone_primary,
        cast(phone_secondary as string)                  as phone_secondary,

        -- Address
        cast(address_street as string)                   as address_street,
        initcap(cast(address_city as string))            as address_city,
        initcap(cast(address_province as string))        as address_province,
        cast(address_postal_code as string)              as address_postal_code,

        -- Banking attributes
        lower(trim(cast(customer_segment as string)))    as customer_segment,
        lower(trim(cast(kyc_status as string)))          as kyc_status,
        cast(kyc_verified_at as timestamp)               as kyc_verified_at,
        cast(onboarding_branch_id as int64)              as onboarding_branch_id,
        lower(trim(cast(acquisition_channel as string))) as acquisition_channel,
        lower(trim(cast(risk_rating as string)))         as risk_rating,
        cast(is_politically_exposed as bool)             as is_politically_exposed,

        -- Derived flags
        cast(deleted_at as timestamp) is not null        as is_deleted,
        cast(deleted_at as timestamp)                    as deleted_at,

        -- Audit timestamps
        cast(created_at as timestamp)                    as created_at,
        cast(updated_at as timestamp)                    as updated_at,
        date(cast(updated_at as timestamp))              as updated_date

    from source
    where cast(deleted_at as timestamp) is null   -- Exclude soft-deleted records
)

select * from renamed
