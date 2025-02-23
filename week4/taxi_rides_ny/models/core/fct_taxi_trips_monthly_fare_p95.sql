{{
    config(
        materialized='view'
    )
}}


with trips_data as (
    select *, FORMAT_TIMESTAMP('%Y-%m', pickup_datetime) AS year_month
     from  {{ ref('fact_trips') }}
   where fare_amount > 0 and trip_distance > 0 and LOWER(payment_type_description) IN ('cash', 'credit card')
)

select distinct service_type,
trips_data.year_month,
PERCENTILE_CONT(fare_amount, 0.97) OVER (PARTITION BY service_type, pickup_year, pickup_month) AS p97,
PERCENTILE_CONT(fare_amount, 0.95) OVER (PARTITION BY service_type, pickup_year, pickup_month) AS p95,
PERCENTILE_CONT(fare_amount, 0.90) OVER (PARTITION BY service_type, pickup_year, pickup_month) AS p90
from trips_data
where year_month = '2020-04'

