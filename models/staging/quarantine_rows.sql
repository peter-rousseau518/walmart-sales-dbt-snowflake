{{ config(materialized='table') }}
-- Silver quarantine: every row that failed a check, with the reason and where it came from.
-- Nothing in this table reaches GOLD. In PROD, a non-empty quarantine triggers an SNS alert
-- (see macros/notify_quarantine.sql).
select
    'department'                                   as source_table,
    store_id,
    dept_id,
    week_date,
    reject_reason,
    _file_name,
    _file_row_number,
    _loaded_at
from {{ ref('chk_department_sales') }}
where reject_reason is not null

union all

select
    'fact'                                         as source_table,
    store_id,
    null                                           as dept_id,
    week_date,
    reject_reason,
    _file_name,
    _file_row_number,
    _loaded_at
from {{ ref('chk_store_features') }}
where reject_reason is not null
