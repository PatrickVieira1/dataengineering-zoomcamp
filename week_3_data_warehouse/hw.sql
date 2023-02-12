CREATE OR REPLACE EXTERNAL TABLE `dtc-de-375902.dezoomcamp1.materialized_nytaxi_fhv_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://prefect-de-zoomcamp_tick/data/fhv/fhv_tripdata_2019-*.csv.gz']
);

SELECT count(*) FROM `dtc-de-375902.dezoomcamp1.nytaxi_fhv_tripdata` LIMIT 100;

/* QUESTION 2 */ 

select count(distinct affiliated_base_number) from `dtc-de-375902.dezoomcamp1.nytaxi_fhv_tripdata`;

select count(distinct affiliated_base_number) from `dtc-de-375902.dezoomcamp1.materiliazed_table`;

/* QUESTION 3 */

select count(*) from `dtc-de-375902.dezoomcamp1.nytaxi_fhv_tripdata`
where PUlocationID is null and DOlocationID is null;

/* QUESTION 4 was to create a partitioned and clustered table which was done by the GUI */

/* QUESTION 5 */

select count(distinct affiliated_base_number) from `dtc-de-375902.dezoomcamp1.materiliazed_table`
where pickup_datetime between '2019-02-28' and '2019-04-01';

select count(distinct affiliated_base_number) from `dtc-de-375902.dezoomcamp1.partitioned_table`
where pickup_datetime between '2019-03-01' and '2019-03-31';