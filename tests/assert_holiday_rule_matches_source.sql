-- The spine derives holiday weeks from calendar rules. Wherever the source also
-- flags holidays, the two must agree. Returns the weeks that disagree (0 rows = pass).
select date_key, holiday_name, is_holiday, source_is_holiday
from {{ ref('dim_date') }}
where source_is_holiday is not null
  and source_is_holiday <> is_holiday
