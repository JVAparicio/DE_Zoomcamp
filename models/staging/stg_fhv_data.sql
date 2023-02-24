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

