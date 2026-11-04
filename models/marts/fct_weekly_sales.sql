-- Grain: one row per store, department and week. Sales only.
select
    store_id      as store_key,
    sales_date    as date_key,
    dept_id,
    weekly_sales
from {{ ref('stg_department_sales') }}
