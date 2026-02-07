with source as (
    SELECT *
    FROM {{ ref('participants') }}
),

final as (
    SELECT
        participant_id,
        participant_code,
        last_name,
        first_name,
        birth_date,
        gender,
        participant_type,
        region_id,
        marital_status
    FROM source
)

select * from final