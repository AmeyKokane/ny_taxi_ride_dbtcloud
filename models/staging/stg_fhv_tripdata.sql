{{ config(materialized='view') }}

/*with tripdata as 
(
  select *,
    row_number() over(partition by Dispatching_base_num, Pickup_datetime) as rn
  from {{ source('staging','fhv_2019_tripdata_external_table') }}
  where Dispatching_base_num is not null 
)*/
select
    -- identifiers
    {{ dbt_utils.surrogate_key(['Dispatching_base_num', 'Pickup_datetime']) }} as tripid,
    Dispatching_base_num,
    cast(pulocationid as integer) as  pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,
    
    -- timestamps
    cast(Pickup_datetime as timestamp) as pickup_datetime,
    cast(DropOff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    cast(SR_Flag as numeric) as SR_Flag
    

from {{ source('staging','fhv_2019_tripdata_external_table') }}
where Dispatching_base_num is not null 


-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=false) %}

  limit 100

{% endif %}