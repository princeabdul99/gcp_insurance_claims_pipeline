with source as (
    SELECT * 
    FROM {{ ref('claims_payment') }}
),

-- As manager, I want to know how long it took the process claim, the total amount and duration to process all payment

agg_payment as (
    SELECT
        claim_payment_code,
        claim_id,
        count(*) as payment_count,
        SUM(payment_amount) as total_amount_paid,
        payment_date
    FROM source
    group by claim_payment_code, claim_id, payment_date
    order by total_amount_paid desc, payment_date 
),

-- I want a table with the total amount paid, total claim paid in each month 
month_payment as (
    select
        DATE_TRUNC(SAFE_CAST(payment_date AS DATE), MONTH) as payment_date,
        count(*) as total_pay_count,
        round(sum(payment_amount),2) as total_payment
    from source
    group by payment_date
    order by payment_date
    -- group by DATE_TRUNC(SAFE_CAST(payment_date AS DATE), MONTH)
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