-- Silver: rename and de-duplicate. Types are already enforced by the load contract.
-- If the same store/department/week arrives twice (a re-sent file), keep the latest load.
select
    store_id,
    dept_id,
    week_date      as sales_date,
    weekly_sales,
    is_holiday
from {{ source('walmart', 'department') }}
qualify row_number() over (
    partition by store_id, dept_id, week_date
    order by _loaded_at desc, _file_row_number desc
) = 1
