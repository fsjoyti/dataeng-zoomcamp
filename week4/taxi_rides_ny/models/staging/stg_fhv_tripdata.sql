
{{
    config(
        materialized='view'
    )
}}

{{config(materialized='view')}}

with tripdata as 
(
  select *
  from {{ source('staging', 'fhv_tripdata') }}
  WHERE dispatching_base_num is not null
)

select
    dispatching_base_num,
    cast(pulocationid as integer) as pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,

    --timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,

    --trip info
    sr_flag,
    affiliated_base_number as affiliated_base_num
from tripdata

{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}