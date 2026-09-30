{{
  config(
    materialized='incremental',
    unique_key='trip_id',
    on_schema_change='sync_all_columns'
  )
}}

-- Data quality: TLC's raw trip files reliably contain a small share (~3%)
-- of physically invalid rows -- negative fares (refunds/corrections that
-- leaked into the export), non-positive trip distance, dropoff before
-- pickup, and a handful of pickup timestamps from well before TLC's
-- electronic record-keeping began (e.g. a stray 2002 date in the raw
-- Jan-2024 file). These are filtered here rather than flagged downstream,
-- since nothing past this point can meaningfully use a trip with a
-- negative fare or a zero-second duration. See tests in
-- _staging__models.yml for the checks that confirm this filter is working.
--
-- The upper bound (pickup_datetime <= var('max_valid_pickup_date')) exists
-- because of a real incident found while loading a full year of data: the
-- June 2024 file contains 2 rows (out of 3.5M) timestamped 2026-06-26 --
-- garbage, two years in the future. Without a filter, those 2 rows became
-- the new high-water mark for the incremental model's `WHERE
-- pickup_datetime > max(already loaded)` check below, which silently
-- excluded every real row from July through December (all correctly dated
-- in 2024, so all "earlier than" the poisoned max) on every subsequent
-- run. No test failed -- row counts, uniqueness, and relationships were
-- all still valid, just missing six months of data. This is the standard
-- failure mode of a naive max()-based incremental filter: it trusts the
-- source's max value completely, so one corrupt outlier poisons every run
-- after it.
--
-- The first fix attempt used `pickup_datetime <= current_date` -- which is
-- wrong, and stayed wrong silently: current_date means "today, wherever
-- this pipeline happens to run," not "today relative to when this data was
-- collected." On a dev machine whose system clock is set to 2026 while
-- loading 2024 files, current_date is 2026-09-30 -- comfortably past the
-- 2026-06-26 poisoned rows, so that filter let them straight through and
-- the bug wasn't actually fixed until this was caught by inspecting the
-- rebuilt data. An explicit var tied to the dataset's own known vintage
-- doesn't depend on the runtime clock meaning anything in particular.
--
-- Fixing the June incident exposed a second, sneakier version of the same
-- problem: the October 2024 file contains exactly one row timestamped
-- 2024-11-14 -- 13 days into the next month, but still a perfectly
-- plausible 2024 date. A global date-range bound can never catch this; the
-- row isn't implausible on its own, it's just in the wrong file. That one
-- row still pushed the incremental watermark 13 days into November, and
-- November's own run then silently skipped everything from Nov 1-14 the
-- same way the June incident skipped July-December.
--
-- The actual fix is the source_file_month check below: each row is
-- validated against the month ITS OWN FILE claims to represent (extracted
-- from the filename via source('nyc_taxi', 'yellow_trips')'s
-- filename=true), not compared against a global date range or the
-- previous incremental run's high-water mark at all. This is the correct
-- mental model for monthly-partitioned source data -- trust the
-- partition, not cross-file timestamp ordering -- and it makes both date
-- bounds above redundant for THIS failure mode (they're kept anyway as a
-- cheap independent check, since they catch a different case: a bad
-- timestamp so extreme it's implausible in any file, like the 2002 and
-- 2026 rows).
-- total_amount is in the surrogate key below for a specific reason: TLC's
-- export contains paired charge/reversal records for cancelled or
-- erroneous trips -- same vendor, timestamps, locations, and fare_amount
-- (usually $0), but opposite-signed total_amount (e.g. -4.00 and +4.00).
-- Without total_amount, those two distinct billing records collide into
-- one trip_id; 83 such collisions showed up as unique-key test failures
-- while building this model, which is how this was caught.
--
-- The QUALIFY below guards against a different issue found the same way,
-- one incremental run later: TLC's raw files occasionally contain a fully
-- identical duplicate row (every single column the same, not just the
-- columns in the surrogate key) -- one such row turned up in the February
-- file. Deduplicating on trip_id here means a repeat of that doesn't
-- re-trigger a unique-key failure on a later run.

with source as (

    select * from {{ source('nyc_taxi', 'yellow_trips') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime',
            'PULocationID', 'DOLocationID', 'fare_amount', 'total_amount'
        ]) }} as trip_id,

        VendorID              as vendor_id,
        tpep_pickup_datetime  as pickup_datetime,
        tpep_dropoff_datetime as dropoff_datetime,
        PULocationID          as pickup_location_id,
        DOLocationID          as dropoff_location_id,
        passenger_count,
        trip_distance,
        RatecodeID            as rate_code_id,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        congestion_surcharge,
        total_amount

    from source
    where
        fare_amount >= 0
        and trip_distance > 0
        and tpep_dropoff_datetime > tpep_pickup_datetime
        and tpep_pickup_datetime >= '2009-01-01'
        and tpep_pickup_datetime <= '{{ var("max_valid_pickup_date") }}'
        -- the real fix: row's pickup month must match the month its own
        -- source file claims to represent (see comment block above)
        and strftime(tpep_pickup_datetime, '%Y-%m') = regexp_extract(filename, 'yellow_tripdata_(\d{4}-\d{2})\.parquet', 1)
    qualify row_number() over (partition by trip_id order by pickup_datetime) = 1

)

select * from renamed

{% if is_incremental() %}
where pickup_datetime > (select max(pickup_datetime) from {{ this }})
{% endif %}
