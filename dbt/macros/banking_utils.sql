-- ============================================================
-- banking_utils.sql — Reusable banking domain macros
-- ============================================================

-- ─────────────────────────────────────────────
-- mask_pii: Mask PII for non-authorized roles
-- Usage: {{ mask_pii('full_name') }} as full_name
-- ─────────────────────────────────────────────
{% macro mask_pii(column_name) %}
    {%- if target.name == 'prod' -%}
        -- In production BigQuery: rely on column-level access policy.
        -- This macro returns the value as-is; BQ policy masking handles
        -- what unauthorized users actually see at query time.
        cast({{ column_name }} as string)
    {%- else -%}
        -- In dev/CI: apply hash-based masking to protect data
        concat(
            substr(cast({{ column_name }} as string), 1, 2),
            '***',
            substr(cast({{ column_name }} as string), -2)
        )
    {%- endif -%}
{% endmacro %}


-- ─────────────────────────────────────────────
-- classify_credit_score: Standard banking bands
-- ─────────────────────────────────────────────
{% macro classify_credit_score(score_column) %}
    case
        when {{ score_column }} >= 800 then 'exceptional'
        when {{ score_column }} >= 740 then 'very_good'
        when {{ score_column }} >= 670 then 'good'
        when {{ score_column }} >= 580 then 'fair'
        else 'poor'
    end
{% endmacro %}


-- ─────────────────────────────────────────────
-- classify_dpd: Days Past Due → Risk class
-- ─────────────────────────────────────────────
{% macro classify_dpd(dpd_column) %}
    case
        when {{ dpd_column }} = 0          then 'current'
        when {{ dpd_column }} between 1 and 30  then 'dpd_1_30'
        when {{ dpd_column }} between 31 and 60 then 'dpd_31_60'
        when {{ dpd_column }} between 61 and 90 then 'dpd_61_90'
        when {{ dpd_column }} > 90         then 'npl'
        else 'unknown'
    end
{% endmacro %}


-- ─────────────────────────────────────────────
-- income_band_sort: Returns sortable order for income ranges
-- ─────────────────────────────────────────────
{% macro income_band_sort(income_range_column) %}
    case {{ income_range_column }}
        when '0-3jt'   then 1
        when '3-5jt'   then 2
        when '5-10jt'  then 3
        when '10-25jt' then 4
        when '25-50jt' then 5
        when '>50jt'   then 6
        else 0
    end
{% endmacro %}


-- ─────────────────────────────────────────────
-- safe_divide: Null-safe division
-- ─────────────────────────────────────────────
{% macro safe_divide(numerator, denominator, default_value=0) %}
    coalesce(safe_divide({{ numerator }}, nullif({{ denominator }}, 0)), {{ default_value }})
{% endmacro %}


-- ─────────────────────────────────────────────
-- is_business_day: Excludes weekends (simplified — no holiday calendar)
-- ─────────────────────────────────────────────
{% macro is_business_day(date_column) %}
    extract(dayofweek from {{ date_column }}) not in (1, 7)  -- 1=Sunday, 7=Saturday in BQ
{% endmacro %}


-- ─────────────────────────────────────────────
-- audit_columns: Standard audit metadata columns
-- Add to every mart model's final select
-- ─────────────────────────────────────────────
{% macro audit_columns() %}
    current_timestamp()                                     as dbt_updated_at,
    '{{ invocation_id }}'                                   as dbt_invocation_id,
    '{{ this.name }}'                                       as dbt_model_name
{% endmacro %}
