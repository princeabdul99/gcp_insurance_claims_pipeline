with claim_decision as (
    SELECT 
        *
    FROM {{ ref('fact_claims_decision') }}    
),

operational_efficiency as (
    SELECT
        ROUND(AVG(claim_cycle_days),2) AS avg_claim_cycle_days,
        AVG(
            DATE_DIFF(first_payment_date, announcement_date, DAY)
        ) AS avg_time_to_first_payment
    FROM claim_decision
)

SELECT * FROM operational_efficiency 