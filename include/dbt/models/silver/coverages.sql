
with source as (
    SELECT * 
    FROM {{ source('ic_bronze', 'coverages') }}
),


renamed as (

    select
        coverid as cover_id,
        covercode as cover_code,
        COALESCE(renewaltype, 'N/A') as renewal_type,
        COALESCE(room, 'N/A') as room,
        participation,
        productcategory as product_category,
        premiummode as premium_mode,
        productdistribution as product_distribution

    from source

)

-- quality_check as (
--     SELECT
--         COUNT(*) AS total_rows,
--         COUNT(DISTINCT cover_id) AS unique_id,
--         SUM(CASE WHEN cover_id IS NULL THEN 1 ELSE 0 END) AS null_ids
--     FROM renamed
-- )

select * from renamed