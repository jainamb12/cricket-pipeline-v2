with cricket as (
    select * from {{ ref('stg_cricket') }}
),
venue as (
    select DISTINCT
        venue_id,
        venue_name,
        venue_city,
        venue_country
    from cricket
)
select * from
venue