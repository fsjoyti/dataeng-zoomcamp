{{
    config(
        materialized='view'
    )
}}

WITH trip_data AS (
    SELECT 
        *,
        TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) AS trip_duration
    FROM  {{ ref('dim_fhv_trips') }}
),
calculated_p90 AS (
    SELECT DISTINCT
        pickup_year,
        pickup_month,
        pickup_locationid,
        dropoff_locationid,
        dropoff_zone,
        pickup_zone,
        PERCENTILE_CONT(trip_duration, 0.90) OVER (PARTITION BY pickup_year, pickup_month, pickup_locationid, dropoff_locationid) AS p90
    FROM trip_data 
    WHERE pickup_zone IN ('Newark Airport', 'SoHo', 'Yorkville East')
    AND pickup_zone != dropoff_zone
    AND pickup_year = 2019 and pickup_month = 11
)
SELECT 
    *,
    RANK() OVER (PARTITION BY pickup_zone ORDER BY p90 DESC) AS p90_rank
FROM calculated_p90
ORDER BY pickup_zone, p90_rank