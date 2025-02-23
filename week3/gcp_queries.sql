
SELECT count(*)  FROM `vibrant-mantis-376307.test_dataset.green_tripdata` where filename like '%green_tripdata_2020%';

SELECT count(*)  FROM `vibrant-mantis-376307.test_dataset.yellow_tripdata` where filename like '%yellow_tripdata_2020%';

SELECT count(*)  FROM `vibrant-mantis-376307.test_dataset.yellow_tripdata` where filename like '%yellow_tripdata_2021_03%';



 SELECT ARRAY_TO_STRING(ARRAY_AGG( sqls), "; " ) FROM ( SELECT CONCAT("drop table ",table_schema,".", table_name ) AS sqls from vibrant-mantis-376307.test_dataset.INFORMATION_SCHEMA.TABLES
where table_name like "yellow_tripdata_%" ORDER BY table_name DESC ) ;

CREATE OR REPLACE EXTERNAL TABLE `vibrant-mantis-376307.test_dataset.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://test-data-lake-bucket/yellow/yellow_tripdata_2019-*.parquet', 'gs://test-data-lake-bucket/yellow/yellow_tripdata_2020-*.parquet']
);

select count(*) from `vibrant-mantis-376307.test_dataset.external_yellow_tripdata`;

CREATE OR REPLACE EXTERNAL TABLE `vibrant-mantis-376307.test_dataset.external_green_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://test-data-lake-bucket/green/green_tripdata_2019-*.parquet', 'gs://test-data-lake-bucket/green/green_tripdata_2020-*.parquet']
);

select count(*) from `vibrant-mantis-376307.test_dataset.external_green_tripdata`;

CREATE OR REPLACE EXTERNAL TABLE `vibrant-mantis-376307.test_dataset.external_fhv_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://test-data-lake-bucket/fhv/fhv_tripdata_2019-*.parquet']
);

select count(*) from  `vibrant-mantis-376307.test_dataset.external_fhv_tripdata`;

drop table test_dataset.yellow_tripdata_partitoned; drop table test_dataset.yellow_tripdata_non_partitoned; drop table test_dataset.yellow_tripdata_clustered; drop table test_dataset.yellow_tripdata_2020_12_ext; drop table test_dataset.yellow_tripdata_2020_12; drop table test_dataset.yellow_tripdata_2020_11_ext; drop table test_dataset.yellow_tripdata_2020_11; drop table test_dataset.yellow_tripdata_2020_10_ext; drop table test_dataset.yellow_tripdata_2020_10; drop table test_dataset.yellow_tripdata_2020_09_ext; drop table test_dataset.yellow_tripdata_2020_09; drop table test_dataset.yellow_tripdata_2020_08_ext; drop table test_dataset.yellow_tripdata_2020_08; drop table test_dataset.yellow_tripdata_2020_07_ext; drop table test_dataset.yellow_tripdata_2020_07; drop table test_dataset.yellow_tripdata_2020_06_ext; drop table test_dataset.yellow_tripdata_2020_06; drop table test_dataset.yellow_tripdata_2020_05_ext; drop table test_dataset.yellow_tripdata_2020_05; drop table test_dataset.yellow_tripdata_2020_04_ext; drop table test_dataset.yellow_tripdata_2020_04; drop table test_dataset.yellow_tripdata_2020_03_ext; drop table test_dataset.yellow_tripdata_2020_03; drop table test_dataset.yellow_tripdata_2020_02_ext; drop table test_dataset.yellow_tripdata_2020_02; drop table test_dataset.yellow_tripdata_2020_01_ext; drop table test_dataset.yellow_tripdata_2020_01; drop table test_dataset.yellow_tripdata_2019_12_ext; drop table test_dataset.yellow_tripdata_2019_12; drop table test_dataset.yellow_tripdata_2019_11_ext; drop table test_dataset.yellow_tripdata_2019_11; drop table test_dataset.yellow_tripdata_2019_10_ext; drop table test_dataset.yellow_tripdata_2019_10; drop table test_dataset.yellow_tripdata_2019_09_ext; drop table test_dataset.yellow_tripdata_2019_09; drop table test_dataset.yellow_tripdata_2019_08_ext; drop table test_dataset.yellow_tripdata_2019_08; drop table test_dataset.yellow_tripdata_2019_07_ext; drop table test_dataset.yellow_tripdata_2019_07; drop table test_dataset.yellow_tripdata_2019_06_ext; drop table test_dataset.yellow_tripdata_2019_06; drop table test_dataset.yellow_tripdata_2019_05_ext; drop table test_dataset.yellow_tripdata_2019_05; drop table test_dataset.yellow_tripdata_2019_04_ext; drop table test_dataset.yellow_tripdata_2019_04; drop table test_dataset.yellow_tripdata_2019_03_ext; drop table test_dataset.yellow_tripdata_2019_03; drop table test_dataset.yellow_tripdata_2019_02_ext; drop table test_dataset.yellow_tripdata_2019_02; drop table test_dataset.yellow_tripdata_2019_01_ext; drop table test_dataset.yellow_tripdata_2019_01;

drop table test_dataset.yellow_tripdata;

drop table test_dataset.external_yellow_tripdata;

CREATE OR REPLACE TABLE `vibrant-mantis-376307.test_dataset.yellow_tripdata_non_partitoned` AS
SELECT * FROM `vibrant-mantis-376307.test_dataset.external_yellow_tripdata`;

CREATE OR REPLACE TABLE `vibrant-mantis-376307.test_dataset.yellow_tripdata_partitoned`
PARTITION BY
DATE(tpep_pickup_datetime)
AS
SELECT * FROM `vibrant-mantis-376307.test_dataset.external_yellow_tripdata`;

select distinct (VendorID) from `vibrant-mantis-376307.test_dataset.yellow_tripdata_non_partitoned` 
where DATE(tpep_pickup_datetime) between '2019-01-01' and '2020-06-30';

select distinct (VendorID) from `vibrant-mantis-376307.test_dataset.yellow_tripdata_partitoned` 
where DATE(tpep_pickup_datetime) between '2019-01-01' and '2020-06-30';

SELECT table_name, partition_id, total_rows
FROM `vibrant-mantis-376307.test_dataset.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'yellow_tripdata_partitoned'
ORDER BY total_rows DESC;

CREATE OR REPLACE TABLE `vibrant-mantis-376307.test_dataset.yellow_tripdata_clustered`
PARTITION BY
DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `vibrant-mantis-376307.test_dataset.external_yellow_tripdata`;

select distinct (VendorID) from `vibrant-mantis-376307.test_dataset.yellow_tripdata_partitoned` 
where DATE(tpep_pickup_datetime) between '2019-01-01' and '2020-06-30' AND VendorID = 1;

select distinct (VendorID) from `vibrant-mantis-376307.test_dataset.yellow_tripdata_clustered` 
where DATE(tpep_pickup_datetime) between '2019-01-01' and '2020-06-30' AND VendorID = 1;

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
