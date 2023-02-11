create or replace external table `de_z_camp_dataset.external_fhv`
options (
  format = 'csv',
  uris = ['gs://dsd_data_zoom_camp_bucket/data/fhv/fhv_tripdata_*.csv.gz']
);


create or replace table `de-z-camp.de_z_camp_dataset.fhv_non_partitoned` as
select * from `de-z-camp.de_z_camp_dataset.external_fhv`;


-- what is the count for fhv vehicle records for year 2019?
select count(coalesce(affiliated_base_number,"null_replace")) from `de-z-camp.de_z_camp_dataset.external_fhv`;

-- write a query to count the distinct number of affiliated_base_number for the entire dataset on both the tables.
select count(distinct affiliated_base_number) from `de-z-camp.de_z_camp_dataset.external_fhv`;

select count(distinct affiliated_base_number) from `de-z-camp.de_z_camp_dataset.fhv_non_partitoned`;


-- how many records have both a blank (null) pulocationid and dolocationid in the entire dataset?
select count(coalesce(affiliated_base_number,"null_replace"))
from `de-z-camp.de_z_camp_dataset.fhv_non_partitoned`
where pulocationid is null and dolocationid is null;


-- partition by pickup_datetime cluster on affiliated_base_number
create or replace table `de-z-camp.de_z_camp_dataset.fhv_partitoned`
partition by date(pickup_datetime)
cluster by affiliated_base_number as
select * from `de-z-camp.de_z_camp_dataset.fhv_non_partitoned`;


-- retrieve the distinct affiliated_base_number between pickup_datetime 2019/03/01 and 2019/03/31 (inclusive)

select distinct affiliated_base_number
from `de-z-camp.de_z_camp_dataset.fhv_non_partitoned`
where date(pickup_datetime) between '2019-03-01' and '2019-03-31';


select distinct affiliated_base_number
from `de-z-camp.de_z_camp_dataset.fhv_partitoned`
where date(pickup_datetime) between '2019-03-01' and '2019-03-31';

