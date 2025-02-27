# dataeng-zoomcamp
See myqueries.sql in week1 folder for sql queries I ran in pgadmin for homework 1 after using homework1.ipynb to load data to postgres.

See data engineering bootcamp homework2.docx in week2 for screenshots of my results from kestra ui. 
For question 3 I ran backfill for yellow data from 2020 Jan 1 to Jan 31 and then queried the row count (after truncating yellow trip data postgres table) . Same in question 4 but for green trip data.
I repeated these steps for gcp scheduled workflow and got same results(without truncating the data).
For question 5 I just ran backfill for march 2021 after truncating the yellow tripdata postgres table and then queried the row count.
Then I repeated this with gcp backfill workflow.

See homework3.docx in week3 folder for screenshots of my result and homework3.sql for queries I ran.

See dlt_workshop folder to see the colab notebook and its outputs.

See homework4.docx  in week4 folder for screenshots of my result and taxi_rides_ny for dbt code. 

See homework5.docx in week5 folder for screenshots of my result and homework5.ipynb for my code.