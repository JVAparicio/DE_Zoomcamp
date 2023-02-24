{{ config(materialized='table') }}

with 

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
),


fhv_data as (
    select *
    from {{ ref('stg_fhv_data') }}
)

select
    fhv_data.pickup_datetime,
    fhv_data.id,
    pickup_zone.zone as pickup_zone,
    dropoff_zone.zone as dropoff_zone
from fhv_data
inner join dim_zones as pickup_zone
on fhv_data.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_data.dropoff_locationid = dropoff_zone.locationid