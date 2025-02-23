
SELECT count(*)  FROM `vibrant-mantis-376307.test_dataset.green_tripdata` where filename like '%green_tripdata_2020%';

SELECT count(*)  FROM `vibrant-mantis-376307.test_dataset.yellow_tripdata` where filename like '%yellow_tripdata_2020%';

SELECT count(*)  FROM `vibrant-mantis-376307.test_dataset.yellow_tripdata` where filename like '%yellow_tripdata_2021_03%';

--query I ran to drop the existing tables

 SELECT ARRAY_TO_STRING(ARRAY_AGG( sqls), "; " ) FROM ( SELECT CONCAT("drop table ",table_schema,".", table_name ) AS sqls from vibrant-mantis-376307.test_dataset.INFORMATION_SCHEMA.TABLES
where table_name like "yellow_tripdata_%" ORDER BY table_name DESC ) ;

CREATE OR REPLACE EXTERNAL TABLE `vibrant-mantis-376307.test_dataset.external_green_tripdata`
(
  VendorID INTEGER,
  lpep_pickup_datetime TIMESTAMP,
  lpep_dropoff_datetime TIMESTAMP,
  store_and_fwd_flag STRING,
  RatecodeID INTEGER,
  PULocationID INTEGER,
  DOLocationID INTEGER,
  passenger_count INTEGER,
  trip_distance FLOAT64,
  fare_amount FLOAT64,
  extra FLOAT64,
  mta_tax FLOAT64,
  tip_amount FLOAT64,
  tolls_amount FLOAT64,
  ehail_fee FLOAT64,
  improvement_surcharge FLOAT64,
  total_amount FLOAT64,
  payment_type INTEGER, 
  trip_type INTEGER,
  congestion_surcharge FLOAT64
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://test-data-lake-bucket/green/green_tripdata_2019-*.parquet', 'gs://test-data-lake-bucket/green/green_tripdata_2020-*.parquet']
);


CREATE OR REPLACE EXTERNAL TABLE `vibrant-mantis-376307.test_dataset.external_yellow_tripdata`
(
  VendorID INTEGER,
  tpep_pickup_datetime TIMESTAMP,
  tpep_dropoff_datetime TIMESTAMP,
  passenger_count INTEGER,
  trip_distance FLOAT64,
  RatecodeID INTEGER,
  store_and_fwd_flag STRING,
  PULocationID INTEGER,
  DOLocationID INTEGER,
  payment_type INTEGER,
  fare_amount FLOAT64,
  extra FLOAT64,
  mta_tax FLOAT64,
  tip_amount FLOAT64,
  tolls_amount FLOAT64,
  improvement_surcharge FLOAT64,
  total_amount FLOAT64,
  congestion_surcharge FLOAT64
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://test-data-lake-bucket/yellow/yellow_tripdata_2019-*.parquet', 'gs://test-data-lake-bucket/yellow/yellow_tripdata_2020-*.parquet']
);