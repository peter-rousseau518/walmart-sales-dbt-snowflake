-- Grain: one row per store, department and week. Sales only.
-- Incremental: each run reads only silver rows loaded since the last run and MERGEs them
-- on the grain, so new weeks are added and corrected weeks replace the old row.
-- Silver keeps the latest load per store/department/week, so the merge source is unique.
-- Rebuild everything with: dbt build --full-refresh --select fct_weekly_sales
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['store_key', 'dept_id', 'date_key'],
    on_schema_change='append_new_columns'
) }}

select
    store_id      as store_key,
    sales_date    as date_key,
    dept_id,
    weekly_sales,
    _loaded_at
from {{ ref('stg_department_sales') }}

{% if is_incremental() %}
  {#- Only filter once the existing table has _loaded_at. On the first incremental run after
      this column was added, every row is merged, which fills _loaded_at for all of them. -#}
  {%- set existing_columns = adapter.get_columns_in_relation(this) | map(attribute='name') | map('upper') | list -%}
  {%- if '_LOADED_AT' in existing_columns %}
where _loaded_at > (
    select coalesce(max(existing._loaded_at), '1900-01-01'::timestamp_ltz)
    from {{ this }} as existing
)
  {%- endif %}
{% endif %}
