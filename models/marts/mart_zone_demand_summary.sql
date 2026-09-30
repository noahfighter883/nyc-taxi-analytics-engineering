-- The table an analyst would actually query: zone-level demand, with
-- human-readable names, ready for a BI tool. Built on top of
-- int_zone_monthly_demand rather than duplicating its logic.

select
    demand.month,
    zones.zone,
    zones.borough,
    demand.trip_count,
    demand.avg_fare,
    demand.avg_trip_duration_minutes,
    demand.demand_tier

from {{ ref('int_zone_monthly_demand') }} demand
left join {{ ref('dim_zones') }} zones on demand.location_id = zones.location_id

order by demand.month, demand.trip_count desc
