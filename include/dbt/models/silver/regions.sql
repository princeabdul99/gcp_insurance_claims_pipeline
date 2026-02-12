
with source as (
    SELECT * 
    FROM {{ source('ic_bronze', 'regions') }}
),

renamed as (

    select
        id as region_id,
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
        time_zone,
        ROW_NUMBER() OVER(PARTITION BY id) AS rn
    from source
),

quality_check as (
    SELECT
        COUNT(*) AS total_rows,
        -- COUNT(DISTINCT region_id) AS unique_id,
        SUM(CASE WHEN state IS NULL THEN 1 ELSE 0 END) AS null_ids
    FROM renamed
),

final as (

    SELECT
        region_id,
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

    FROM renamed
    where rn = 1
)

select * from final