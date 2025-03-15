SELECT table_schema,table_name 
FROM information_schema.tables 
ORDER BY table_schema,table_name;

select count(*) from public.yellow_tripdata_staging;

select count(*) from public.yellow_tripdata;

truncate table public.yellow_tripdata;

select count(*) from public.green_tripdata;

truncate table public.green_tripdata;

