{{ config(materialized='table') }}

with fhv_tripdata as (
    select *, 
        'FHV' as service_type 
    from {{ ref('stg_fhv_tripdata') }}
), 

dim_zones as (
    select * 
    from {{ ref('dim_zones') }}
    where borough!= 'Unknown'
) 


select *
from fhv_tripdata
inner join dim_zones as pickup_zone
on fhv_tripdata.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_tripdata.dropoff_locationid = dropoff_zone.locationid
where pickup_locationid IS NOT NULL AND dropoff_locationid IS NOT NULL