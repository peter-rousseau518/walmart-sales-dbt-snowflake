-- Silver: rows that passed every check in chk_department_sales.
-- Rejected rows (duplicates, unknown stores, missing sales) go to quarantine_rows instead.
select
    store_id,
    dept_id,
    week_date      as sales_date,
    weekly_sales,
    is_holiday
from {{ ref('chk_department_sales') }}
where reject_reason is null
