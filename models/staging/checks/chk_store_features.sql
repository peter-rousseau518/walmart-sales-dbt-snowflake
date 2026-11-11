{{ config(materialized='ephemeral') }}
-- Row-level checks for store-week conditions. reject_reason NULL = good row.
with src as (
    select
        *,
        row_number() over (
            partition by store_id, week_date
            order by _loaded_at desc, _file_row_number desc
        ) as load_rank
    from {{ source('walmart', 'fact') }}
),

known_stores as (
    select store_id from {{ ref('stg_stores') }}
)

select
    src.*,
    case
        when src.load_rank > 1     then 'duplicate store/week (older load)'
        when ks.store_id is null   then 'unknown store_id'
    end as reject_reason
from src
left join known_stores ks on ks.store_id = src.store_id
