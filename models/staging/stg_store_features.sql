-- Silver: rename and de-duplicate (latest load wins per store and week).
-- 'NA' values already arrive as NULL from the load contract.
select
    store_id,
    week_date      as feature_date,
    temperature,
    fuel_price,
    markdown1,
    markdown2,
    markdown3,
    markdown4,
    markdown5,
    cpi,
    unemployment,
    is_holiday
from {{ source('walmart', 'fact') }}
qualify row_number() over (
    partition by store_id, week_date
    order by _loaded_at desc, _file_row_number desc
) = 1
