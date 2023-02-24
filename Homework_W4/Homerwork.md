## Question 1:
What is the count of records in the model fact_trips after running all models with the test run variable disabled and filtering for 2019 and 2020 data only (pickup datetime)?

A: 
    61648442

Code:

Run the following statement in your DWH

    select count(*) from `ds_zoomcamp.fact_trips` where  DATE_TRUNC(pickup_datetime, YEAR) in ('2019-01-01', '2020-01-01');


## Question 2:
What is the distribution between service type filtering by years 2019 and 2020 data as done in the videos?

A:

![alt text](<image.png>)



## Question 3

What is the count of records in the model stg_fhv_tripdata after running all models with the test run variable disabled (:false)?

A:

    43244696

Code:

Create the dbt model - stg_fhv_data.sql

    {{ config(materialized="view") }}

    select
        -- identifiers
        cast(int64_field_0 as integer) as id,
        cast(PUlocationID as integer) as  pickup_locationid,
        cast(DOlocationID as integer) as dropoff_locationid,

        -- timestamps
        cast(pickup_datetime as timestamp) as pickup_datetime,
        cast(dropOff_datetime as timestamp) as dropoff_datetime,

        -- trip info
        cast(SR_Flag as string) as SR_Flag,
        cast(dispatching_base_num as string) as dispatching_base_num,
        cast(Affiliated_base_number as string) as Affiliated_base_number,


        from {{ source('staging','fhv_tripdata') }}


    -- dbt build --m <model.sql> --var 'is_test_run: false'
    {% if var('is_test_run', default=true) %}

     limit 100

    {% endif %}
 

Run it

    dbt run --select stg_fhv_data --var 'is_test_run: false'

Check results on Big Query

    SELECT count(*) FROM `dtc-de-course-375312.ds_zoomcamp.stg_fhv_data`;    



## Question 4
What is the count of records in the model fact_fhv_trips after running all dependencies with the test run variable disabled (:false)?

A:

    22998722

Code:
Create the dbt model - fact_fhv_trips.sql

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


Run it

    dbt run --select fact_fhv_trips --var 'is_test_run: false'

Check results on Big Query

    SELECT count(*) FROM `dtc-de-course-375312.ds_zoomcamp.fact_fhv_trips` 



## Question 5
What is the month with the biggest amount of rides after building a tile for the fact_fhv_trips table?

![alt text2](<image2.png>)