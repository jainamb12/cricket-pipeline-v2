with cricket as (
    select * from {{ ref('stg_cricket') }}
),
drs as (
    SELECT distinct
        drs_review_id,
        drs_review_by,
        drs_original_decision,
        drs_outcome
    from cricket
)
select *
from drs