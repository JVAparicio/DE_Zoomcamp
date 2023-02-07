CREATE OR REPLACE EXTERNAL TABLE `ds-zoomcamp.trips.fhv_tripdata`
OPTIONS (
  format = 'CSV.GZ',
  uris = ['gs://prefect-de-course-zoomcamp/data/fhv_tripdata_2019-*.csv.gz']