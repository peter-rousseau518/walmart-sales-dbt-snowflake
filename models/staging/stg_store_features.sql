-- Silver: rows that passed every check in chk_store_features.
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
from {{ ref('chk_store_features') }}
where reject_reason is null
