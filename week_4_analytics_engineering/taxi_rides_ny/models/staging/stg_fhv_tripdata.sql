{{ config(materialized='view') }}

with tripdata as 
(
  select *,
  --row_number() over(partition by dispatching_base_num, pickup_datetime, dropOff_datetime) as rn
  from {{ source('staging','fhv_partitioned') }}
  --where dispatching_base_num is not null 
)
select
    -- identifiers
    --{{ dbt_utils.surrogate_key(['vendorid', 'lpep_pickup_datetime']) }} as tripid,
    cast(dispatching_base_num as integer) as dispatching_base_num,
    cast(PUlocationID as integer) as  PUlocationID,
    cast(DOlocationID as integer) as DOlocationID,
    
    -- timestamps
    --cast(pickup_datetime as timestamp) as pickup_datetime,
    --cast(dropOff_datetime as timestamp) as dropOff_datetime,
    pickup_datetime,
    dropOff_datetime,
    
    -- trip info
    cast(SR_Flag as integer) as SR_Flag,
    cast(Affiliated_base_number as integer) as Affiliated_base_number
    
from tripdata
--where rn = 1


-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
