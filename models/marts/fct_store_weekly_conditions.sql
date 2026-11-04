-- Grain: one row per store and week. Markdowns and outside conditions live here,
-- once per store-week, so they can be summed or averaged without double counting.
select
    store_id      as store_key,
    feature_date  as date_key,
    temperature,
    fuel_price,
    markdown1,
    markdown2,
    markdown3,
    markdown4,
    markdown5,
    coalesce(markdown1, 0) + coalesce(markdown2, 0) + coalesce(markdown3, 0)
      + coalesce(markdown4, 0) + coalesce(markdown5, 0)              as markdown_total,
    coalesce(markdown1, markdown2, markdown3, markdown4, markdown5) is not null
                                                                    as has_markdown_data,
    cpi,
    unemployment
from {{ ref('stg_store_features') }}
