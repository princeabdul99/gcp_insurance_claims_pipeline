with source as (
    SELECT * 
    FROM {{ ref('claims_announcement') }}
),


regions as (
    select * from {{ ref('dim_regions')}}
),

participant as (
    select * from {{ ref('dim_participants')}}
),

part_reg as (
    select 
        p.participant_id,
        r.state,
        r.state_geo_region
    from participant p
    inner join regions r
        on p.region_id = r.region_id
),

claim_by_geo_reg as (
    select 
        count(*) as cnt,
        sum(last_forecast_amount) as total_amt_forcast,
    from source
    group by claim_id
    having total_amt_forcast > 2000
),

-- As manager, I want to know how long it took to file claim, amount forcasted and duration to process all payment
test_date_diff as (

    select
        claim_id,
        announcement_date,
        event_date,
        closing_date,
        last_forecast_amount,
        (announcement_date - event_date) as last_announce,
        (closing_date - announcement_date) as process_period

    from source

),

-- I want a table with the total amount forcasted for claims, total claim filed in each month 
month_claim as (
    select
        DATE_TRUNC(SAFE_CAST(announcement_date AS DATE), MONTH) as announcement_date,
        count(*) as total_filed_claim,
        round(sum(last_forecast_amount),2) as total_payment,
        round(min(last_forecast_amount),2) as min_amount_forecasted,
        round(max(last_forecast_amount),2) as max_amount_forecasted
    from source
    group by announcement_date
    order by announcement_date
),



final as (

    select
        claim_id,
        claim_code,
        policy_id,
        policy_code,
        announcement_date,
        event_date,
        closing_date,
        last_forecast_amount,
        broker_id,
        insured_id,
        product_id

    from source
)

select * from final