with source as (
    SELECT *
    FROM {{ ref('coverages') }}
),

final as (
    SELECT
        cover_id,
        cover_code,
        renewal_type,
        room,
        participation,
        product_category,
        premium_mode,
        product_distribution
    FROM source
)

select * from final