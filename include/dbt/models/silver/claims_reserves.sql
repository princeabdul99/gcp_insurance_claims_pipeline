with source as (

    select * from {{ source('ic_bronze', 'claims_reserves') }}

),

renamed as (

    select
        claimid as claim_id,
        claimcode as claim_code,
        policyid as policy_id,
        announcementdate as announcement_date,
        closingdate as closing_date,
        coverid as cover_id,
        provisionamount as provision_amount,
        provisiondate as provision_date,
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

final as (

    SELECT
        claim_id,
        claim_code,
        policy_id,
        announcement_date,
        closing_date,
        cover_id,
        provision_amount,
        provision_date,
        broker_id,
        insured_id,
        product_id
    FROM renamed
)

select * from final