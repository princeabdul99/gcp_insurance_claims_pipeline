with source as (
    SELECT *
    FROM {{ ref('participants') }}
),

region as (
    select * 
    from {{ ref('regions')}}
),

joined_region as (
    select
        count(*) as count,
        r.state,
        ROUND(AVG(r.median_income),2) as avg_income,
        MAX(r.median_income) as high_income,
        MIN(r.median_income) as low_income,
        (select count(*) from source) as totalcount
    from source s
    left join region r
        on s.region_id = r.region_id

    group by r.state
    order by r.state asc  
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