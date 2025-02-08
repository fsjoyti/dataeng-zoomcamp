
8. False

5.Partition by tpep\_dropoff\_datetime and Cluster on VendorID

7.GCP bucket

6. 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table

![](data:image/png;base64...)

![](data:image/png;base64...)

4. 8333![](data:image/png;base64...)

2. 0 MB for the External Table and 155.12 MB for the Materialized Table

![](data:image/png;base64...)

![](data:image/png;base64...)

1.

* 20,332,093

![](data:image/png;base64...)

3.

* BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

![](data:image/png;base64...)

![](data:image/png;base64...)