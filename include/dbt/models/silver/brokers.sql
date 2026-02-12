
with source as (
    SELECT *
    FROM {{ source('ic_bronze', 'brokers') }}
),
renamed as (

    SELECT
        SAFE_CAST(
            SAFE_CAST(brokerid AS FLOAT64)
            AS INT64
        ) AS broker_id,
        brokercode AS broker_code,
        brokerfullname AS broker_fullname,
        COALESCE( distributionnetwork, 'N/A') AS distribution_network,
        distributionchannel AS distribution_channel,
        COALESCE( commissionscheme, 'N/A') AS commission_scheme,
        ROW_NUMBER() OVER(PARTITION BY brokerid) AS rn

    FROM source
    WHERE brokerid IS NOT NULL
),


-- quality_check as (
--     SELECT
--         COUNT(*) AS total_rows,
--         COUNT(DISTINCT broker_id) AS unique_id,
--         SUM(CASE WHEN broker_id IS NULL THEN 1 ELSE 0 END) AS null_ids
--     FROM renamed
-- )

final as (

    select
        broker_id,
        broker_code,
        broker_fullname,
        distribution_network,
        distribution_channel,
        commission_scheme
    from renamed
    where rn = 1
)

select * from final

