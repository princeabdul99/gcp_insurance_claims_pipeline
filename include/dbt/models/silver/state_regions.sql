
with source as (
    SELECT * 
    FROM {{ source('ic_bronze', 'state_regions') }}
),

renamed as (

    select
        string_field_0 as state_code,
        string_field_1 as state_name,
        string_field_2 as state_geo_region

    from source
)

-- quality_check as (
--     SELECT
--         COUNT(*) AS total_rows,
--         COUNT(DISTINCT state_code) AS unique_id,
--         SUM(CASE WHEN state_code IS NULL THEN 1 ELSE 0 END) AS null_ids
--     FROM renamed
-- )

select * from renamed 
