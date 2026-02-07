with source as (
    SELECT * 
    FROM {{ ref('claims_announcement') }}
),

final as (

    select
        claim_id,
        claim_code,
        policy_id,
        policy_code,
        announcement_date,
        event_date,
        closing_date,
        last_forecast_amount,
        broker_id,
        insured_id,
        product_id

    from source
)

select * from final