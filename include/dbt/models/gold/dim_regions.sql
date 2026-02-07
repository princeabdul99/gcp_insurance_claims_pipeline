with source as (
    SELECT * 
    FROM {{ ref('regions') }}
),

state_region as (
    SELECT * 
    FROM {{ ref('state_regions') }}
),

geo_region as (
    SELECT
        state_code,
        state_geo_region
    FROM state_region
),


final as (
    SELECT
        id,
        name,
        county,
        state_code,
        state,
        type,
        geo_region.state_geo_region,
        latitude,
        longitude,
        area_code,
        population,
        households,
        median_income,
        land_area,
        water_area,
        time_zone

    FROM source
    LEFT JOIN geo_region USING (state_code)
)

select * from final