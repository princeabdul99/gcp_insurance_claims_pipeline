

with source as (
    SELECT * 
    FROM {{ source('ic_bronze', 'participants') }}
),

renamed as (

    select
        participantid as participant_id,
        participantcode as participant_code,
        lastname as last_name,
        coalesce(firstname, 'N/A') as first_name,
        coalesce(birthdate, '1900-01-01') as birth_date,
        coalesce(initcap(gender), 'N/A') as gender,
        initcap(participanttype) as participant_type,
        regionid as region_id,
        coalesce(maritalstatus, 'N/A') as marital_status,
        ROW_NUMBER() OVER(PARTITION BY participantid) AS rn

    from source
),

-- quality_check as (
--     SELECT
--         COUNT(*) AS total_rows,
--         -- COUNT(DISTINCT participant_id) AS unique_id,
--         SUM(CASE WHEN first_name IS NULL THEN 1 ELSE 0 END) AS null_ids
--     FROM renamed
-- ),

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

    FROM renamed
    where rn = 1
)

select * from final
