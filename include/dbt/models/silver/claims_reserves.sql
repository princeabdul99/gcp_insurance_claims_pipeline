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
        brokerid as broker_id,
        insuredid as insured_id,
        productid as product_id

    from source
)

select * from renamed