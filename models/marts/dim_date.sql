-- One row per retail week (week ending Friday), generated as a calendar spine.
-- Covers Feb 2010 - Dec 2015 so the calendar keeps growing past the data instead of
-- depending on which weeks happen to appear in a source file.
with spine as (
    {{ dbt_utils.date_spine(
        datepart="week",
        start_date="cast('2010-02-05' as date)",
        end_date="cast('2016-01-01' as date)"
    ) }}
),

weeks as (
    select cast(date_week as date) as week_end_date
    from spine
),

-- Holiday weeks follow the source's retail calendar rules (week ending Friday):
--   Super Bowl   = Friday after the first Sunday of February  -> Feb 6-12
--   Labor Day    = Friday after the first Monday of September -> Sep 5-11
--   Thanksgiving = Friday after the fourth Thursday of November -> Nov 23-29
--   Christmas    = week containing Dec 25                       -> Dec 25-31
flagged as (
    select
        week_end_date,
        case
            when month(week_end_date) = 2  and dayofmonth(week_end_date) between 6 and 12  then 'Super Bowl'
            when month(week_end_date) = 9  and dayofmonth(week_end_date) between 5 and 11  then 'Labor Day'
            when month(week_end_date) = 11 and dayofmonth(week_end_date) between 23 and 29 then 'Thanksgiving'
            when month(week_end_date) = 12 and dayofmonth(week_end_date) between 25 and 31 then 'Christmas'
        end as holiday_name
    from weeks
),

source_holidays as (
    select feature_date, boolor_agg(is_holiday) as source_is_holiday
    from {{ ref('stg_store_features') }}
    group by feature_date
),

sales_weeks as (
    select distinct sales_date
    from {{ ref('stg_department_sales') }}
)

select
    f.week_end_date                       as date_key,
    f.week_end_date                       as full_date,
    dateadd(day, -6, f.week_end_date)     as week_start_date,
    year(f.week_end_date)                 as year,
    quarter(f.week_end_date)              as quarter,
    month(f.week_end_date)                as month,
    monthname(f.week_end_date)            as month_name,
    weekofyear(f.week_end_date)           as week_of_year,
    f.holiday_name,
    f.holiday_name is not null            as is_holiday,
    s.sales_date is not null              as has_sales,
    h.source_is_holiday                   -- kept for the holiday-rule reconciliation test
from flagged f
left join sales_weeks     s on s.sales_date   = f.week_end_date
left join source_holidays h on h.feature_date = f.week_end_date
