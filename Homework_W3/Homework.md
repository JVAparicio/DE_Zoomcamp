## Week 3 Homework


## Question 1:
What is the count for fhv vehicle records for year 2019?

A: 

    43,244,696

Code:
    
    CREATE OR REPLACE EXTERNAL TABLE `dtc-de-course-375312.ds_zoomcamp.fhv_tripdata`
    OPTIONS (
    format = 'CSV',
    uris = ['gs://prefect-de-course-zoomcamp/data/fhv_tripdata_2019-*.csv.gz']);

    CREATE OR REPLACE TABLE `dtc-de-course-375312.ds_zoomcamp.fhv_tripdata_non_partitoned ` AS
    SELECT * FROM `dtc-de-course-375312.ds_zoomcamp.fhv_tripdata`;

    SELECT count(*) FROM `dtc-de-course-375312.ds_zoomcamp.fhv_tripdata`;



## Question 2:
Write a query to count the distinct number of affiliated_base_number for the entire dataset on both the tables.</br> 
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

A: 

    0 MB for the External Table and 317.94MB for the BQ Table

Code:

    SELECT distinct(affiliated_base_number) FROM `dtc-de-course-375312.ds_zoomcamp.fhv_tripdata_non_partitoned `;

    SELECT distinct(affiliated_base_number) FROM `dtc-de-course-375312.ds_zoomcamp.fhv_tripdata`;    

## Question 3:
How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?

A: 

    717,748

Code:

    SELECT count(*) as null_count FROM `dtc-de-course-375312.ds_zoomcamp.fhv_tripdata_non_partitoned `where PUlocationID is null
    and DOlocationID is null;

## Question 4:
What is the best strategy to optimize the table if query always filter by pickup_datetime and order by affiliated_base_number?

A:

    Partition by pickup_datetime Cluster on affiliated_base_number


## Question 5:
Implement the optimized solution you chose for question 4. Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 03/01/2019 and 03/31/2019 (inclusive).</br> 
Use the BQ table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? Choose the answer which most closely matches.

A:

    647.87 MB for non-partitioned table and 23.06 MB for the partitioned table


Code:

    CREATE OR REPLACE TABLE `dtc-de-course-375312.ds_zoomcamp.fhv_tripdata_partitioned_clustered`
    PARTITION BY DATE(pickup_datetime)
    CLUSTER BY affiliated_base_number AS
    SELECT * FROM `dtc-de-course-375312.ds_zoomcamp.fhv_tripdata_non_partitoned `;

    SELECT DISTINCT(affiliated_base_number) from `dtc-de-course-375312.ds_zoomcamp.fhv_tripdata_partitioned_clustered`
    where DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';

    SELECT DISTINCT(affiliated_base_number) from `dtc-de-course-375312.ds_zoomcamp.fhv_tripdata_non_partitoned `
    where DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';


## Question 6: 
Where is the data stored in the External Table you created?

A: 
    GCP Bucket


## Question 7:
It is best practice in Big Query to always cluster your data:
A:

    False
