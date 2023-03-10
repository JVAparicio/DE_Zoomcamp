## Week 1 Homework

### Question 1.
Which tag has the following text?

#### A:
iidfile string

#### Code:
docker build --help

### Question 2:  
How many python packages/modules are installed?

#### A:
3

#### Code:
Run the command: "pip list" inside the container

Package    Version
- pip        22.0.4
* setuptools 58.1.0
+ wheel      0.38.4



### Question 3: 
How many taxi trips were totally made on January 15?

#### A:
20530

#### Code: 
select count(*) from green_taxi_data ytd
where date_trunc('day',lpep_pickup_datetime) = '2019-01-15' and date_trunc('day',lpep_dropoff_datetime) = '2019-01-15';



### Question 4: 
Which was the day with the largest trip distance?

#### A:
2019-01-15

#### Code:
select  lpep_pickup_datetime from green_taxi_data ytd where trip_distance = (select max(trip_distance) from green_taxi_data);


### Question 5:
In 2019-01-01 how many trips had 2 and 3 passengers?

#### A:
2: 1282 ; 3: 254

#### Code:
select passenger_count, count(*) from green_taxi_data
where date_trunc('day',lpep_pickup_datetime) = '2019-01-01'
group by passenger_count;


### Question 6: 
For the passengers picked up in the Astoria Zone which was the drop up zone that had the largest tip?

#### A:
Long Island City/Queens Plaza

#### Code:
select gtd.lpep_pickup_datetime, tip_amount, pickup_location."Zone", dropoff_location."Zone"  from green_taxi_data gtd
left join zones pickup_location on gtd."PULocationID" = pickup_location."LocationID"
left join zones dropoff_location on gtd."DOLocationID" = dropoff_location."LocationID"
where pickup_location."Zone" = 'Astoria'
order by tip_amount desc
limit 1;
