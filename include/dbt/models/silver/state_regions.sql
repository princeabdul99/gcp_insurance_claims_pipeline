
with source as (
    SELECT * 
    FROM {{ source('ic_bronze', 'state_regions') }}
),

renamed as (

    select
        StateCode as state_code,
        State as state_name,
        Region as state_geo_region,
        ROW_NUMBER() OVER(PARTITION BY StateCode) AS rn

    from source
),

-- quality_check as (
--     SELECT
--         COUNT(*) AS total_rows,
--         COUNT(DISTINCT state_code) AS unique_id,
--         SUM(CASE WHEN state_code IS NULL THEN 1 ELSE 0 END) AS null_ids
--     FROM renamed
-- ),

final as (

    SELECT
        state_code,
        state_name,
        state_geo_region

    FROM renamed
    where rn = 1
)

select * from final

