with cricket as (
    select * from {{ ref('stg_cricket') }}
),
match as (
    select distinct
    match_id,
    match_date,
    match_format,
    tournament_name,
    season,
    match_stage,
    toss_decision
    from cricket
)
select * from match