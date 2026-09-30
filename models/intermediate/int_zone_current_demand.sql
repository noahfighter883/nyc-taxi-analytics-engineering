-- "Current state" view of each zone's demand tier -- the month with the
-- most total trip volume in int_zone_monthly_demand. This is what actually
-- gets snapshotted: each time this model is rebuilt after a new month's
-- data lands, "current" shifts forward, and dbt's snapshot compares the
-- new current state to the last one it recorded per zone.
--
-- Deliberately NOT max(month): TLC's monthly files always contain a
-- handful of stray trips with pickup timestamps just past the month
-- boundary (e.g. 3 trips timestamped Feb 1st in the January file). Picking
-- the chronologically latest month would make "current" a near-empty
-- one-day sliver instead of the month the file actually represents --
-- picking by trip volume is robust to that.

with monthly as (

    select * from {{ ref('int_zone_monthly_demand') }}

),

month_totals as (

    select month, sum(trip_count) as total_trips
    from monthly
    group by 1

),

current_month as (

    select month
    from month_totals
    order by total_trips desc
    limit 1

)

select monthly.*
from monthly
inner join current_month on monthly.month = current_month.month
