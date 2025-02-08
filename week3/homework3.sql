--create external parquet table
CREATE OR REPLACE EXTERNAL TABLE `vibrant-mantis-376307.test_dataset.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://test-data-lake-bucket/yellow_tripdata_2024-*.parquet']
);

--Create a (regular/materialized) table in BQ using the Yellow Taxi Trip Records (do not partition or cluster this table).
CREATE OR REPLACE TABLE `vibrant-mantis-376307.test_dataset.yellow_tripdata_non_partitoned` AS
SELECT * FROM `vibrant-mantis-376307.test_dataset.external_yellow_tripdata`;

--question 2
select count(PULocationID) from `test_dataset.external_yellow_tripdata`;
select count(PULocationID) from `test_dataset.yellow_tripdata_non_partitoned`;

--question 3
select PULocationID from `test_dataset.yellow_tripdata_non_partitoned`;

select PULocationID, DOLocationID from `test_dataset.yellow_tripdata_non_partitoned`;

-- Question 4
SELECT COUNT(*)
FROM `test_dataset.yellow_tripdata_non_partitoned`
WHERE fare_amount = 0;

-- Question 5 
CREATE OR REPLACE TABLE `vibrant-mantis-376307.test_dataset.yellow_tripdata_clustered`
PARTITION BY
DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `vibrant-mantis-376307.test_dataset.external_yellow_tripdata`;

-- Question 6
SELECT distinct (VendorID) FROM `test_dataset.yellow_tripdata_non_partitoned`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

SELECT distinct (VendorID) FROM `test_dataset.yellow_tripdata_clustered`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
