with claim_decision as (
    SELECT 
        *
    FROM {{ ref('fact_claims_decision') }}    
),

reserve_adequacy AS (
    SELECT
        claim_id,
        latest_reserve_amount,
        total_paid_amount,

        CASE
            WHEN total_paid_amount > latest_reserve_amount * 1.2
                THEN 'UNDER_RESERVED'
            WHEN total_paid_amount < latest_reserve_amount * 0.8
                THEN 'OVER_RESERVED'
            ELSE 'ADEQUATE'
        END AS reserve_adequacy_decision
    FROM claim_decision
)

SELECT * FROM reserve_adequacy 