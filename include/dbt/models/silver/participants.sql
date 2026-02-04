

with source as (
    SELECT * 
    FROM {{ source('ic_bronze', 'participants') }}
),

renamed as (

    select
        participantid as participant_id,
        participantcode as participant_code,
        lastname as last_name,
        firstname as first_name,
        birthdate as birth_date,
        initcap(gender) as gender,
        initcap(participanttype) as participant_type,
        regionid as region_id,
        maritalstatus as marital_status

    from source
)

-- quality_check as (
--     SELECT
--         COUNT(*) AS total_rows,
--         COUNT(DISTINCT participant_id) AS unique_id,
--         SUM(CASE WHEN participant_id IS NULL THEN 1 ELSE 0 END) AS null_ids
--     FROM renamed
-- )

select * from renamed
