with source as (
    SELECT *
    FROM {{ ref('policies') }}
),

final as (
    SELECT
        policy_id,
        policy_code,
        policy_inception_date,
        cancelation_date,
        policy_start_date,
        policy_expiration_date,
        renewal_month,
        annualized_policy_premium,
        policy_status,
        customer_id,
        insured_id,
        product_id,
        broker_id
    FROM source
)

select * from final