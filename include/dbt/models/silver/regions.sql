
with source as (
    SELECT * 
    FROM {{ source('ic_bronze', 'regions') }}
),

renamed as (

    select
        id,
        name,
        county,
        state_code,
        state,
        type,
        latitude,
        longitude,
        area_code,
        population,
        households,
        median_income,
        land_area,
        water_area,
        time_zone

    from source
)

-- quality_check as (
--     SELECT
--         COUNT(*) AS total_rows,
--         COUNT(DISTINCT longitude) AS unique_id,
--         SUM(CASE WHEN longitude IS NULL THEN 1 ELSE 0 END) AS null_ids
--     FROM renamed
-- )

select * from renamed
