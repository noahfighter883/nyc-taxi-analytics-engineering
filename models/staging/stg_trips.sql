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
    qualify row_number() over (partition by trip_id order by pickup_datetime) = 1

)

select * from renamed

{% if is_incremental() %}
where pickup_datetime > (select max(pickup_datetime) from {{ this }})
{% endif %}
