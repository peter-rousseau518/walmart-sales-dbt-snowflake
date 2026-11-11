{{ config(severity='warn') }}
-- Warns (does not fail the build) when rows were quarantined for a reason other than
-- an older duplicate load. Returns the offending rows; 0 rows = nothing to review.
select *
from {{ ref('quarantine_rows') }}
where reject_reason not like 'duplicate%'
