with source as (
    SELECT *
    FROM {{ ref('brokers') }}
),

final as (
    SELECT
        broker_id,
        broker_code,
        broker_fullname,
        distribution_network,
        distribution_channel,
        commission_scheme
    FROM source
)

select * from final