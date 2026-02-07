with source as (
    SELECT * 
    FROM {{ ref('claims_reserves') }}
),

final as (

    select
        claim_id,
        claim_code,
        policy_id,
        announcement_date,
        closing_date,
        cover_id,
        provision_amount,
        provision_date,
        broker_id,
        insured_id,
        product_id

    from source
)

select * from final