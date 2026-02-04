
with source as (
    SELECT * 
    FROM {{ source('ic_bronze', 'products') }}
),

renamed as (

    select
        productid as product_id,
        productcategory as product_category,
        productsubcategory as product_sub_category,
        product

    from source
)

-- quality_check as (
--     SELECT
--         COUNT(*) AS total_rows,
--         COUNT(DISTINCT product_id) AS unique_id,
--         SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) AS null_ids
--     FROM renamed
-- )

select * from renamed 
