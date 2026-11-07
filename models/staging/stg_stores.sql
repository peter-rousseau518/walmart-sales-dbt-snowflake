-- Silver: rename and de-duplicate (latest load wins).
select
    store_id,
    store_type,
    store_size
from {{ source('walmart', 'stores') }}
qualify row_number() over (
    partition by store_id
    order by _loaded_at desc, _file_row_number desc
) = 1
