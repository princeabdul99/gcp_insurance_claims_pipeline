with claim_decision as (
    SELECT 
        *
    FROM {{ ref('fact_claims_decision') }}    
),


decision_handling AS (
    SELECT
        *,
        CASE
            WHEN policy_active_flag = 0 THEN 'ESCALATE'
            WHEN paid_to_reserve_ratio > 1.2 THEN 'ESCALATE'
            WHEN forecast_to_premium_ratio <= 0.5
                AND payment_count <= 1
                THEN 'AUTO_APPROVE'
            ELSE 'MANUAL_REVIEW'
        END AS claim_handling_decision
    FROM claim_decision
)

SELECT * FROM decision_handling 