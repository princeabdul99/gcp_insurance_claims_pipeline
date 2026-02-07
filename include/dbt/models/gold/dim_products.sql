with source as (
    SELECT *
    FROM {{ ref('products') }}
),

final as (
    SELECT
        product_id,
        product_category,
        product_sub_category,
        product
    FROM source
)

select * from final