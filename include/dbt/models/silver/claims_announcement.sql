
with source as (

    select * from {{ source('ic_bronze', 'claims_announcement') }}

),

renamed as (

    select
        claimid as claim_id,
        claimcode as claim_code,
        policyid as policy_id,
        policycode as policy_code,
        announcementdate as announcement_date,
        eventdate as event_date,
        closingdate as closing_date,
        lastforecastamount as last_forecast_amount,
        brokerid as broker_id,
        insuredid as insured_id,
        productid as product_id

    from source

)

-- quality_check as (
--     SELECT
--         COUNT(*) AS total_rows,
--         COUNT(DISTINCT closing_date) AS unique_id,
--         SUM(CASE WHEN closing_date IS NULL THEN 1 ELSE 0 END) AS null_ids
--     FROM renamed
-- )
select * from renamed