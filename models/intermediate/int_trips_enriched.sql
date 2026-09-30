with trips as (

    select * from {{ ref('stg_trips') }}

),

pickup_zones as (

    select * from {{ ref('stg_zones') }}

),

dropoff_zones as (

    select * from {{ ref('stg_zones') }}

),

enriched as (

    select
        trips.*,

        pickup_zones.borough  as pickup_borough,
        pickup_zones.zone     as pickup_zone,
        dropoff_zones.borough as dropoff_borough,
        dropoff_zones.zone    as dropoff_zone,

        date_diff('minute', trips.pickup_datetime, trips.dropoff_datetime) as trip_duration_minutes,
        date_trunc('month', trips.pickup_datetime)                        as pickup_month,

        case
            when trips.fare_amount > 0
                then round(trips.tip_amount / trips.fare_amount * 100, 1)
            else null
        end as tip_pct

    from trips
    left join pickup_zones  on trips.pickup_location_id  = pickup_zones.location_id
    left join dropoff_zones on trips.dropoff_location_id = dropoff_zones.location_id

)

select * from enriched
