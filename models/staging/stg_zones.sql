with source as (

    select * from {{ source('nyc_taxi_zones', 'zone_lookup') }}

),

renamed as (

    select
        LocationID   as location_id,
        Borough      as borough,
        Zone         as zone,
        service_zone

    from source

)

select * from renamed
