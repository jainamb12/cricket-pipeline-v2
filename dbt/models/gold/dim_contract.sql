with cricket as (
    select * from {{ ref('stg_cricket') }}
),
contract as (
    select DISTINCT
        contract_id,
        contract_grade,
        contract_status,
        annual_retainer_usd
    from cricker
)
select * from contract