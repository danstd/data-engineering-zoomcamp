create or replace external table `de_z_camp_dataset.external_fhv`
options (
  format = 'CSV',
  uris = ['gs://dsd_data_zoom_camp_bucket/data/fhv/fhv_tripdata_*.csv.gz']
);


CREATE OR REPLACE TABLE `de-z-camp.de_z_camp_dataset.fhv_non_partitoned` AS
SELECT * FROM `de-z-camp.de_z_camp_dataset.external_fhv`;


-- What is the count for fhv vehicle records for year 2019?
SELECT count(coalesce(affiliated_base_number,"null_replace")) FROM `de-z-camp.de_z_camp_dataset.external_fhv`;

-- Write a query to count the distinct number of affiliated_base_number for the entire dataset on both the tables.
SELECT count(distinct affiliated_base_number) FROM `de-z-camp.de_z_camp_dataset.external_fhv`;

SELECT count(distinct affiliated_base_number) FROM `de-z-camp.de_z_camp_dataset.fhv_non_partitoned`;


-- How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?
select count(coalesce(affiliated_base_number,"null_replace"))
from `de-z-camp.de_z_camp_dataset.fhv_non_partitoned`
where PUlocationID is null and DOlocationID is null;


-- Partition by pickup_datetime Cluster on affiliated_base_number
CREATE OR REPLACE TABLE `de-z-camp.de_z_camp_dataset.fhv_partitoned`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number AS
SELECT * FROM `de-z-camp.de_z_camp_dataset.fhv_non_partitoned`;


-- Retrieve the distinct affiliated_base_number between pickup_datetime 2019/03/01 and 2019/03/31 (inclusive)

select distinct affiliated_base_number
from `de-z-camp.de_z_camp_dataset.fhv_non_partitoned`
where date(pickup_datetime) between '2019-03-01' and '2019-03-31';


select distinct affiliated_base_number
from `de-z-camp.de_z_camp_dataset.fhv_partitoned`
where date(pickup_datetime) between '2019-03-01' and '2019-03-31';

