with source as (
    SELECT * 
    FROM {{ ref('claims_payment') }}
),

final as (

    select
        claim_payment_code,
        claim_id,
        claim_code,
        policy_id,
        announcement_date,
        event_date,
        closing_date,
        cover_id,
        payment_date,
        payment_amount,
        broker_id,
        insured_id,
        product_id

    from source
)

select * from final