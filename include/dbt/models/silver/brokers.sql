
with source as (
    SELECT *
    FROM {{ source('ic_bronze', 'brokers') }}
),
renamed as (

    select
        brokerid as broker_id,
        brokercode as broker_code,
        brokerfullname as broker_fullname,
        distributionnetwork as distribution_network,
        distributionchannel as distribution_channel,
        COALESCE( commissionscheme, 'N/A') as commission_scheme

    from source

)

-- quality_check as (
--     SELECT
--         COUNT(*) AS total_rows,
--         COUNT(DISTINCT broker_id) AS unique_id,
--         SUM(CASE WHEN broker_id IS NULL THEN 1 ELSE 0 END) AS null_ids
--     FROM renamed
-- )

select * from renamed 

