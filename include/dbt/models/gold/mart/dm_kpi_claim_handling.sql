with claim_decision as (
    SELECT 
        *
    FROM {{ ref('dm_decision_claim_handling') }}    
),
kpi as (
    SELECT
        claim_handling_decision,
        COUNT(*) AS claim_count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2)
            AS decision_percentage
    FROM claim_decision
    GROUP BY claim_handling_decision

)

SELECT * FROM kpi