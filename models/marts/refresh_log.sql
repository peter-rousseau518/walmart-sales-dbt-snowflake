-- One row per successful dbt build. The dashboard reads max(refreshed_at) to show
-- "Data as of" and to clear its cache when new data has been built.
-- The depends_on lines make this run last: dbt build skips it if any mart or mart test fails,
-- so a row here always means the gold tables were rebuilt and tested.
-- depends_on: {{ ref('fct_weekly_sales') }}
-- depends_on: {{ ref('fct_store_weekly_conditions') }}
-- depends_on: {{ ref('dim_store') }}
-- depends_on: {{ ref('dim_date') }}
{{ config(materialized='incremental') }}

select
    current_timestamp()::timestamp_ltz as refreshed_at,
    '{{ invocation_id }}'               as invocation_id,
    '{{ target.name }}'                 as target_name
