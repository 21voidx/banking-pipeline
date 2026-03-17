{% snapshot snap_customers %}

{{
    config(
        target_schema='snapshots',
        strategy='timestamp',
        unique_key='customer_id',
        updated_at='updated_at',
        invalidate_hard_deletes=True,
        description='SCD Type 2 snapshot of customer master data. Tracks segment upgrades, KYC changes, risk rating changes.',
        meta={
            "owner": "data-engineering",
            "retention": "permanent",
            "sensitivity": "HIGH — contains PII"
        }
    )
}}

select
    customer_id,
    customer_uuid,
    full_name,
    national_id,
    date_of_birth,
    gender,
    customer_segment,     -- Track segment upgrades (retail → priority → premier)
    kyc_status,           -- Track KYC lifecycle
    risk_rating,          -- Track risk rating changes (critical for compliance)
    income_range,
    address_province,
    acquisition_channel,
    is_politically_exposed,
    created_at,
    updated_at

from {{ source('core_banking', 'customers') }}
where deleted_at is null

{% endsnapshot %}
