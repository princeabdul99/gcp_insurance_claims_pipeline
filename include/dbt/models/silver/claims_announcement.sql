
with source as (

    select * from {{ source('ic_bronze', 'claims_announcement') }}

),

renamed as (

    select
        SAFE_CAST(
            SAFE_CAST(claimid AS FLOAT64)
            AS INT64
        ) AS claim_id,
        claimcode as claim_code,
        SAFE_CAST(
            SAFE_CAST(policyid AS FLOAT64)
            AS INT64
        ) AS policy_id,
        policycode as policy_code,
        announcementdate as announcement_date,
        eventdate as event_date,
        closingdate as closing_date,
        lastforecastamount as last_forecast_amount,
        SAFE_CAST(
            SAFE_CAST(brokerid AS FLOAT64)
            AS INT64
        ) AS broker_id,
        
        SAFE_CAST(
            SAFE_CAST(insuredid AS FLOAT64)
            AS INT64
        ) AS insured_id,
        
        SAFE_CAST(
            SAFE_CAST(productid AS FLOAT64)
            AS INT64
        ) AS product_id,

    from source

),
-- quality_check as (
--     SELECT
--         COUNT(*) AS total_rows,
--         -- COUNT(DISTINCT last_forecast_amount) AS unique_id,
--         SUM(CASE WHEN last_forecast_amount IS NULL THEN 1 ELSE 0 END) AS null_ids
--     FROM renamed
-- ),

final as (

    SELECT
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
    FROM renamed
)

select * from final