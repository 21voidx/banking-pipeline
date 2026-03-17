-- ============================================================
-- banking_utils.sql — Reusable banking domain macros
-- ============================================================

-- ─────────────────────────────────────────────────────────────
-- mask_pii
-- Masks PII fields for dev/CI. In prod, BigQuery column-level
-- access policies handle masking at query time.
-- Usage: {{ mask_pii('full_name') }} as full_name
-- ─────────────────────────────────────────────────────────────
{% macro mask_pii(column_name) %}
    {%- if target.name == 'prod' -%}
        cast({{ column_name }} as string)
    {%- else -%}
        concat(
            substr(cast({{ column_name }} as string), 1, 2),
            '***',
            substr(cast({{ column_name }} as string), -2)
        )
    {%- endif -%}
{% endmacro %}


-- ─────────────────────────────────────────────────────────────
-- classify_credit_score
-- Maps numeric score to standard banking band.
-- Usage: {{ classify_credit_score('score_value') }}
-- ─────────────────────────────────────────────────────────────
{% macro classify_credit_score(score_column) %}
    case
        when {{ score_column }} >= 800 then 'exceptional'
        when {{ score_column }} >= 740 then 'very_good'
        when {{ score_column }} >= 670 then 'good'
        when {{ score_column }} >= 580 then 'fair'
        else 'poor'
    end
{% endmacro %}


-- ─────────────────────────────────────────────────────────────
-- classify_dpd
-- Maps Days Past Due to OJK/BI risk classification bucket.
-- Usage: {{ classify_dpd('dpd') }}
-- ─────────────────────────────────────────────────────────────
{% macro classify_dpd(dpd_column) %}
    case
        when {{ dpd_column }} = 0                   then 'current'
        when {{ dpd_column }} between 1  and 30     then 'dpd_1_30'
        when {{ dpd_column }} between 31 and 60     then 'dpd_31_60'
        when {{ dpd_column }} between 61 and 90     then 'dpd_61_90'
        when {{ dpd_column }} > 90                  then 'npl'
        else 'unknown'
    end
{% endmacro %}


-- ─────────────────────────────────────────────────────────────
-- income_band_sort
-- Returns sortable integer for Indonesian income range labels.
-- Usage: order by {{ income_band_sort('income_range') }}
-- ─────────────────────────────────────────────────────────────
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


-- ─────────────────────────────────────────────────────────────
-- is_business_day
-- Returns true if date is Monday–Friday (no holiday calendar).
-- Usage: {{ is_business_day('transaction_date') }}
-- ─────────────────────────────────────────────────────────────
{% macro is_business_day(date_column) %}
    extract(dayofweek from {{ date_column }}) not in (1, 7)
    -- BigQuery: 1=Sunday, 7=Saturday
{% endmacro %}


-- ─────────────────────────────────────────────────────────────
-- audit_columns
-- Standard dbt run metadata columns. Add to every mart's
-- final SELECT to enable lineage and run tracking in BigQuery.
-- Usage: {{ audit_columns() }}
-- ─────────────────────────────────────────────────────────────
{% macro audit_columns() %}
    current_timestamp()         as dbt_updated_at,
    '{{ invocation_id }}'       as dbt_invocation_id,
    '{{ this.name }}'           as dbt_model_name,
    '{{ target.name }}'         as dbt_target
{% endmacro %}


-- ─────────────────────────────────────────────────────────────
-- get_execution_date
-- Returns the Airflow execution date passed via --vars,
-- falling back to yesterday for manual runs.
-- Usage: {{ get_execution_date() }}
-- ─────────────────────────────────────────────────────────────
{% macro get_execution_date() %}
    {%- if var('execution_date', none) is not none -%}
        date('{{ var("execution_date") }}')
    {%- else -%}
        date_sub(current_date(), interval 1 day)
    {%- endif -%}
{% endmacro %}


-- ─────────────────────────────────────────────────────────────
-- incremental_filter
-- Standard incremental predicate for BigQuery partitioned tables.
-- Uses execution_date var when available, else looks back 3 days
-- to handle late-arriving data.
-- Usage (in incremental models):
--   {% if is_incremental() %}
--     where updated_at >= {{ incremental_filter() }}
--   {% endif %}
-- ─────────────────────────────────────────────────────────────
{% macro incremental_filter(lookback_days=3) %}
    {%- if var('execution_date', none) is not none -%}
        timestamp(date_sub(date('{{ var("execution_date") }}'), interval {{ lookback_days }} day))
    {%- else -%}
        timestamp(date_sub(current_date(), interval {{ lookback_days }} day))
    {%- endif -%}
{% endmacro %}
