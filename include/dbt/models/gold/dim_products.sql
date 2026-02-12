with source as (
    SELECT *
    FROM {{ ref('products') }}
),

-- group_category as (
--     select
--         count(*) as count,
--         product_category,
--         (select count(*) from source) as total
--     from source
--     group by product_category    
-- ),

final as (
    SELECT
        product_id,
        product_category,
        product_sub_category,
        product
    FROM source
)

select * from final