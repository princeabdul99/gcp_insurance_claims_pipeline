with reserve_adequacy as (
    SELECT 
        *
    FROM {{ ref('dm_decision_reserve_adequacy') }}    
),

-- DECISION KPI (MEASUREMENT) => Reserve Accuracy
reserve_accuracy as (
    SELECT
        reserve_adequacy_decision,
        COUNT(*) AS claim_count
    FROM reserve_adequacy
    GROUP BY reserve_adequacy_decision
)

SELECT * FROM reserve_accuracy