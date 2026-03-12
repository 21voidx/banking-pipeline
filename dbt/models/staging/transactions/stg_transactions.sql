{{
    config(
        materialized='view',
        description='Staged transactions from MySQL. Core financial fact view.',
    )
}}

with source as (
    select * from {{ source('transactions', 'transactions') }}
),

transaction_types as (
    select * from {{ source('transactions', 'transaction_types') }}
),

payment_methods as (
    select * from {{ source('transactions', 'payment_methods') }}
),

renamed as (
    select
        -- Keys
        cast(t.transaction_id as int64)         as transaction_id,
        cast(t.transaction_uuid as string)      as transaction_uuid,
        cast(t.account_id as int64)             as account_id,
        cast(t.customer_id as int64)            as customer_id,
        cast(t.merchant_id as int64)            as merchant_id,

        -- Type enrichment
        cast(t.type_id as int64)                as type_id,
        cast(tt.type_code as string)            as transaction_type_code,
        cast(tt.type_name as string)            as transaction_type_name,
        cast(tt.type_category as string)        as transaction_category,

        -- Payment method
        cast(t.method_id as int64)              as method_id,
        cast(pm.method_code as string)          as payment_method_code,
        cast(pm.method_name as string)          as payment_method_name,

        -- Financial amounts
        cast(t.amount as numeric)               as amount,
        cast(t.currency as string)              as currency,
        cast(t.exchange_rate as numeric)        as exchange_rate,
        cast(t.amount_idr as numeric)           as amount_idr,
        cast(t.fee_amount as numeric)           as fee_amount,
        cast(t.amount_idr as numeric)
            + cast(t.fee_amount as numeric)     as total_amount_idr,

        -- Balance
        cast(t.balance_before as numeric)       as balance_before,
        cast(t.balance_after as numeric)        as balance_after,

        -- Metadata
        cast(t.description as string)           as description,
        cast(t.reference_number as string)      as reference_number,
        lower(trim(cast(t.channel as string)))  as channel,

        -- Status
        lower(trim(cast(t.transaction_status as string))) as transaction_status,
        cast(t.status_reason as string)         as status_reason,
        cast(t.transaction_status as string) = 'completed' as is_completed,
        cast(t.transaction_status as string) = 'failed'    as is_failed,
        cast(t.transaction_status as string) = 'reversed'  as is_reversed,

        -- Direction flag (from customer perspective)
        case
            when lower(cast(tt.type_category as string)) = 'credit'  then 'credit'
            when lower(cast(tt.type_category as string)) = 'debit'   then 'debit'
            when lower(cast(tt.type_category as string)) = 'fee'     then 'debit'
            when lower(cast(tt.type_category as string)) = 'reversal' then 'credit'
            else 'unknown'
        end                                     as amount_direction,

        -- Timestamps
        cast(t.transaction_date as date)        as transaction_date,
        cast(t.transaction_at as timestamp)     as transaction_at,
        cast(t.processed_at as timestamp)       as processed_at,
        extract(hour from cast(t.transaction_at as timestamp))   as transaction_hour,
        extract(dayofweek from cast(t.transaction_at as timestamp)) as transaction_day_of_week,
        cast(t.created_at as timestamp)         as created_at,
        cast(t.updated_at as timestamp)         as updated_at

    from source t
    left join transaction_types tt on t.type_id = tt.type_id
    left join payment_methods pm on t.method_id = pm.method_id
)

select * from renamed
