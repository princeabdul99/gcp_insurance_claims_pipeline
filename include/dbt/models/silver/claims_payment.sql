with source as (

    select * from {{ source('ic_bronze', 'claims_payment') }}

),

renamed as (

    select
        claimpaymentcode as claim_payment_code,
        claimid as claim_id,
        claimcode as claim_code,
        policyid as policy_id,
        announcementdate as announcement_date,
        eventdate as event_date,
        closingdate as closing_date,
        coverid as cover_id,
        paymentdate as payment_date,
        paymentamount as payment_amount,
        brokerid as broker_id,
        insuredid as insured_id,
        productid as product_id

    from source

)

select * from renamed