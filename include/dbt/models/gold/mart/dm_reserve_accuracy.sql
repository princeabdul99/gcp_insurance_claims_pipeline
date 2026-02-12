with reserve_adequacy as (
    SELECT 
        *
    FROM {{ ref('dm_reserve_adequacy_decision') }}    
),

reserve_accuracy as (
    SELECT
        reserve_adequacy_decision,
        COUNT(*) AS claim_count
    FROM reserve_adequacy
    GROUP BY reserve_adequacy_decision
)

SELECT * FROM reserve_accuracy