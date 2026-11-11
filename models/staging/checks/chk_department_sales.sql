{{ config(materialized='ephemeral') }}
-- Row-level checks for weekly sales. Every row gets a reject_reason (NULL = good).
-- stg_department_sales keeps the good rows; quarantine_rows keeps the rest.
with src as (
    select
        *,
        row_number() over (
            partition by store_id, dept_id, week_date
            order by _loaded_at desc, _file_row_number desc
        ) as load_rank
    from {{ source('walmart', 'department') }}
),

known_stores as (
    select store_id from {{ ref('stg_stores') }}
)

select
    src.*,
    case
        when src.load_rank > 1           then 'duplicate store/dept/week (older load)'
        when ks.store_id is null         then 'unknown store_id'
        when src.weekly_sales is null    then 'missing weekly_sales'
    end as reject_reason
from src
left join known_stores ks on ks.store_id = src.store_id
