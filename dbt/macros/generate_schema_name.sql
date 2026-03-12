-- ============================================================
-- generate_schema_name.sql
-- ============================================================
-- Custom schema naming: ensures dev/prod separation.
-- Dev:  dev_<schema>    (e.g., dev_staging, dev_marts_customer)
-- Prod: <schema>        (e.g., staging, marts_customer)
-- ============================================================

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}

    {%- if target.name == 'prod' -%}
        {# Production: use schema exactly as configured in dbt_project.yml #}
        {%- if custom_schema_name is none -%}
            {{ default_schema }}
        {%- else -%}
            {{ custom_schema_name | trim }}
        {%- endif -%}

    {%- elif target.name == 'ci' -%}
        {# CI: prefix with ci_<pipeline_id> to isolate runs #}
        {%- if custom_schema_name is none -%}
            ci_{{ default_schema }}
        {%- else -%}
            ci_{{ custom_schema_name | trim }}
        {%- endif -%}

    {%- else -%}
        {# Dev: prefix with dev_ to avoid polluting prod datasets #}
        {%- if custom_schema_name is none -%}
            dev_{{ default_schema }}
        {%- else -%}
            dev_{{ custom_schema_name | trim }}
        {%- endif -%}
    {%- endif -%}

{%- endmacro %}
