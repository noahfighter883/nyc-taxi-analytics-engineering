{#
  Why this is the SCD Type 2 candidate in this project, not the zone
  lookup table: taxi zones themselves (LocationID -> borough/zone name)
  essentially never change, so snapshotting them would just produce one
  static row per zone forever -- technically an SCD2 table, but nothing
  for it to demonstrate. A zone's DEMAND TIER (int_zone_current_demand),
  on the other hand, genuinely changes month to month as real trip data
  lands -- that's the entity worth tracking history for, and it's exactly
  the kind of thing a real analytics team would want to know ("has this
  zone's demand classification shifted?").

  Strategy is `check` rather than `timestamp` because this is a derived
  aggregate with no natural updated_at column -- dbt compares the listed
  check_cols to the last snapshotted row per unique_key and inserts a new
  row (closing out the old one) only when one of them actually changed.

  check_cols is deliberately just `demand_tier`, not also trip_count/
  avg_fare: those two drift by small amounts almost every month for nearly
  every zone, which would make nearly every zone "change" on every run and
  bury the signal that actually matters here -- did this zone's discrete
  classification (High/Medium/Low) move. A first pass that included all
  three found 252 of 260 zones "changed" between Jan and Feb, which turned
  out to mean "trip_count moved by a few trips," not "demand shifted" --
  narrowing to just demand_tier is what makes the snapshot's history
  actually answer the question it's meant to answer.
#}

{% snapshot zone_demand_tier_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='location_id',
        strategy='check',
        check_cols=['demand_tier'],
    )
}}

select * from {{ ref('int_zone_current_demand') }}

{% endsnapshot %}
