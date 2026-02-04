
with source as (
    SELECT * 
    FROM {{ source('ic_bronze', 'policies') }}
),


renamed as (

    select
        policyid as policy_id,
        policycode as policy_code,
        policyinceptiondate as policy_inception_date,
        COALESCE(cancelationdate, '2099-12-31') as cancelation_date,
        policystartdate as policy_start_date,
        policyexpirationdate as policy_expiration_date,
        renewalmonth as renewal_month,
        annualizedpolicypremium as annualized_policy_premium,
        policystatus as policy_status,
        customerid as customer_id,
        insuredid as insured_id,
        productid as product_id,
        brokerid as broker_id

    from source
)

-- quality_check as (
--     SELECT
--         COUNT(*) AS total_rows,
--         COUNT(DISTINCT policy_id) AS unique_id,
--         SUM(CASE WHEN policy_id IS NULL THEN 1 ELSE 0 END) AS null_ids
--     FROM renamed
-- )

select * from renamed
