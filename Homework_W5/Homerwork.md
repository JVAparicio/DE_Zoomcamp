## Question 1
What's the output of spark version?

A:

    3.3.2

Code:

    import pyspark
    pyspark.__version__


## Question 2
What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)?


A:
    24MB

Code:

    schema = types.StructType([
        types.StructField('dispatching_base_num', types.StringType(), True),
        types.StructField('pickup_datetime', types.TimestampType(), True),
        types.StructField('dropoff_datetime', types.TimestampType(), True),
        types.StructField('PULocationID', types.IntegerType(), True),
        types.StructField('DOLocationID', types.IntegerType(), True),
        types.StructField('SR_Flag', types.StringType(), True),
        types.StructField('Affiliated_base_number', types.DoubleType(), True),
    ])

    df = spark.read \
        .schema(schema)\
        .option('header', 'true')\
        .csv('fhvhv_tripdata_2021-06.csv.gz')

    df.repartition(12).write.parquet('data/fhv_tripdata/2021/06/', mode='overwrite')


    On terminal:
        du -sh data/fhv_tripdata/2021/06/*



## Question 3
How many taxi trips were there on June 15?


A:

    452,470

Code:

    from pyspark.sql import functions as F
    df \
        .withColumn('pickup_date', F.to_date(df.pickup_datetime)) \
        .filter("pickup_date = '2021-06-15'") \
        .count()

    or

    df.createOrReplaceTempView('trips_fhv')

    df_result = spark.sql("""
        SELECT count(*) from trips_fhv
        where date_trunc("Day", pickup_datetime) = '2021-06-15'
        """).show()



## Question 4
How long was the longest trip in Hours?

A:

    66.87 Hours

Code:

    df \
        .withColumn("DiffInSeconds", F.col("dropoff_datetime").cast("long") - F.col("pickup_datetime").cast("long"))\
        .withColumn("DiffInHours",F.col("DiffInSeconds")/3600)\
        .select(F.max("DiffInHours"))\
        .show()

    or

        df_result = spark.sql("""
        with trip_time_table as (
            SELECT (unix_timestamp(dropoff_datetime) - unix_timestamp(pickup_datetime))/3600 AS trip_time
            from trips_fhv)

        SELECT MAX(trip_time) from trip_time_table
        """).show()  


## Question 5
Spark’s User Interface which shows application's dashboard runs on which local port?

A:

    4040

## Question 6
Using the zone lookup data and the fhvhv June 2021 data, what is the name of the most frequent pickup location zone?

A:

    Crown Heights North

Code:

    df_trips = df.join(df_zones, df.PULocationID == df_zones.LocationID)

    df_trips.groupBy("Zone")\
        .agg(F.count("Zone").alias("Total_Trips"))\
        .sort(F.desc("Total_Trips"))\
        .show()