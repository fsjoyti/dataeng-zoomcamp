
SELECT count(*)  FROM `vibrant-mantis-376307.test_dataset.green_tripdata` where filename like '%green_tripdata_2020%';

SELECT count(*)  FROM `vibrant-mantis-376307.test_dataset.yellow_tripdata` where filename like '%yellow_tripdata_2020%';

SELECT count(*)  FROM `vibrant-mantis-376307.test_dataset.yellow_tripdata` where filename like '%yellow_tripdata_2021_03%';

--query I ran to drop the existing tables

 SELECT ARRAY_TO_STRING(ARRAY_AGG( sqls), "; " ) FROM ( SELECT CONCAT("drop table ",table_schema,".", table_name ) AS sqls from vibrant-mantis-376307.test_dataset.INFORMATION_SCHEMA.TABLES
where table_name like "yellow_tripdata_%" ORDER BY table_name DESC ) ;
