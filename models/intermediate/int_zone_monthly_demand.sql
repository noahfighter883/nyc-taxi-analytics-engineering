-- One row per pickup zone per month, with a demand tier (High/Medium/Low,
-- by trip-count tercile within that month). This is the entity that
-- actually changes over time in this dataset -- a zone's relative demand
-- shifts month to month -- which is why it's the thing tracked by
-- snapshots/zone_demand_tier_snapshot.sql (see that file and
-- int_zone_current_demand.sql for why this is the SCD Type 2 candidate
-- here rather than the mostly-static zone lookup table itself).

with enriched as (

    select * from {{ ref('int_trips_enriched') }}

),

monthly_counts as (

    select
        pickup_location_id as location_id,
        pickup_month        as month,
        count(*)                                     as trip_count,
        round(avg(fare_amount), 2)                    as avg_fare,
        round(avg(trip_duration_minutes), 1)          as avg_trip_duration_minutes

    from enriched
    group by 1, 2

),

tiered as (

    select
        *,
        ntile(3) over (partition by month order by trip_count) as demand_tile
    from monthly_counts

)

select
    location_id,
    month,
    trip_count,
    avg_fare,
    avg_trip_duration_minutes,
    case demand_tile
        when 3 then 'High'
        when 2 then 'Medium'
        else 'Low'
    end as demand_tier

from tiered
