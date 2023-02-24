## Question 1:
What is the count of records in the model fact_trips after running all models with the test run variable disabled and filtering for 2019 and 2020 data only (pickup datetime)?

A: 
    61648442

Code:

Run the following statement in your DWH

    select count(*) from `ds_zoomcamp.fact_trips` where  DATE_TRUNC(pickup_datetime, YEAR) in ('2019-01-01', '2020-01-01');


## Question 2:
What is the distribution between service type filtering by years 2019 and 2020 data as done in the videos?

A:

![alt text](<image.png>)



## Question 3

