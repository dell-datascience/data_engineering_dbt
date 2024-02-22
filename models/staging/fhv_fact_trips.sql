{{ config(materialized='table') }}

with fhv_tripdata as (
    select *, 
        'FHV' as service_type 
    from {{ ref('stg_fhv_tripdata') }}
), 

dim_zone as (
    select * 
    from {{ ref('dim_zones') }}
    where borough!= 'Unknown'
) 

select fhv_tripdata.*,
        pickup_zone.borough as pickup_borough,
        pickup_zone.zone as pickup_zone,
        pickup_zone.service_zone as pickup_service_zone,
        dropoff_zone.borough as dropoff_borough,
        dropoff_zone.zone as dropoff_zone,
        dropoff_zone.service_zone as dropoff_service_zone,

from fhv_tripdata
inner join dim_zone as pickup_zone
on fhv_tripdata.pickup_locationid = pickup_zone.locationid
inner join dim_zone as dropoff_zone
on fhv_tripdata.dropoff_locationid = dropoff_zone.locationid
-- where fhv_tripdata.pickup_locationid IS NOT NULL OR fhv_tripdata.dropoff_locationid IS NOT NULL