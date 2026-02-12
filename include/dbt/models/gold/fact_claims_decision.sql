with claim_base AS (
    SELECT
        claim_id,
        policy_id,
        broker_id,
        insured_id,
        product_id,
        announcement_date,
        event_date,
        closing_date,
        last_forecast_amount

    FROM {{ ref('claims_announcement') }}
),

payments AS (
    SELECT
        -- claim_payment_code,
        claim_id,
        SUM(payment_amount) AS total_paid_amount,
        COUNT(*) AS payment_count,
        MIN(payment_date) AS first_payment_date,
        MAX(payment_date) AS last_payment_date

    FROM {{ ref('claims_payment') }}
    GROUP BY claim_id
),

reserves AS (
    SELECT
        claim_id,
        MAX(provision_amount) AS latest_reserve_amount
    FROM {{ ref('claims_reserves') }}
    GROUP BY claim_id
),

policy AS (
    SELECT
        policy_id,
        annualized_policy_premium,
        policy_status
    FROM {{ ref('dim_policies') }}
), 




final AS (
    SELECT
        clmbs.claim_id,
        clmbs.policy_id,
        clmbs.broker_id,
        clmbs.insured_id,
        clmbs.product_id,

        clmbs.announcement_date,
        clmbs.closing_date,

        DATE_DIFF(
            COALESCE(clmbs.closing_date, CURRENT_DATE),
            clmbs.announcement_date,
            DAY
        ) AS claim_cycle_days,
        
        clmbs.last_forecast_amount,
        pol.annualized_policy_premium,

        pay.total_paid_amount,
        pay.payment_count,
        pay.first_payment_date,

        res.latest_reserve_amount,

        -- Decision Features
        ROUND(SAFE_DIVIDE(
            clmbs.last_forecast_amount,
            pol.annualized_policy_premium
        ),2) AS forecast_to_premium_ratio,

        ROUND(SAFE_DIVIDE(
            pay.total_paid_amount,
            res.latest_reserve_amount
        ),2) AS paid_to_reserve_ratio,
        
        CASE WHEN pol.policy_status = 'Active' THEN 1 ELSE 0 END AS policy_active_flag


    FROM claim_base clmbs
    LEFT JOIN payments pay ON clmbs.claim_id = pay.claim_id
    LEFT JOIN reserves res ON clmbs.claim_id = res.claim_id
    LEFT JOIN policy pol ON clmbs.policy_id = pol.policy_id

    -- WHERE pol.policy_status = 'Active'

)

-- quality_check as (
--     SELECT
--         COUNT(*) AS total_rows,
--         COUNT(DISTINCT claim_id) AS unique_id,
--         SUM(CASE WHEN claim_id IS NULL THEN 1 ELSE 0 END) AS null_ids
--     FROM claim_base
-- ),

SELECT * FROM final