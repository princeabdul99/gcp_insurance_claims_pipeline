
with source as (
    SELECT * 
    FROM {{ source('ic_bronze', 'policies') }}
),


renamed as (

    select
        SAFE_CAST(
            SAFE_CAST(policyid AS FLOAT64)
            AS INT64
        ) AS policy_id,
        policycode as policy_code,
        policyinceptiondate as policy_inception_date,
        -- COALESCE(cancelationdate, '2099-12-31') as cancelation_date,
        cancelationdate as cancelation_date,
        policystartdate as policy_start_date,
        policyexpirationdate as policy_expiration_date,
        renewalmonth as renewal_month,
        annualizedpolicypremium as annualized_policy_premium,
        policystatus as policy_status,
        customerid as customer_id,
        insuredid as insured_id,
        productid as product_id,
        brokerid as broker_id,
        ROW_NUMBER() OVER(PARTITION BY policyid) AS rn

    from source
),

-- quality_check as (
--     SELECT
--         COUNT(*) AS total_rows,
--         COUNT(DISTINCT policy_id) AS unique_id,
--         SUM(CASE WHEN policy_id IS NULL THEN 1 ELSE 0 END) AS null_ids
--     FROM renamed
-- ),

final as (

    SELECT
        policy_id,
        policy_code,
        policy_inception_date,
        cancelation_date,
        policy_start_date,
        policy_expiration_date,
        renewal_month,
        annualized_policy_premium,
        policy_status,
        customer_id,
        insured_id,
        product_id,
        broker_id

    FROM renamed
    where rn = 1
)

select * from final
