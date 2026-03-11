-- created_at: 2026-03-11T12:56:16.587940+00:00
-- finished_at: 2026-03-11T12:56:16.594937+00:00
-- elapsed: 6ms
-- outcome: success
-- dialect: duckdb
-- node_id: not available
-- query_id: not available
-- desc: Get table schema
DESCRIBE "energy"."main"."fct_energy_features";
-- created_at: 2026-03-11T12:56:16.587939+00:00
-- finished_at: 2026-03-11T12:56:16.594970+00:00
-- elapsed: 7ms
-- outcome: success
-- dialect: duckdb
-- node_id: not available
-- query_id: not available
-- desc: Get table schema
DESCRIBE "energy"."main"."stg_energy";
-- created_at: 2026-03-11T12:56:16.596907+00:00
-- finished_at: 2026-03-11T12:56:16.597187+00:00
-- elapsed: 280us
-- outcome: success
-- dialect: duckdb
-- node_id: not available
-- query_id: not available
-- desc: Get table schema
DESCRIBE "energy"."main"."stg_weather";
-- created_at: 2026-03-11T12:56:16.839840+00:00
-- finished_at: 2026-03-11T12:56:16.847+00:00
-- elapsed: 7ms
-- outcome: success
-- dialect: duckdb
-- node_id: test.dbt_energy.not_null_fct_energy_features_lag_7.406da43aae
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.dbt_energy.not_null_fct_energy_features_lag_7.406da43aae", "profile_name": "dbt_energy", "target_name": "dev"} */
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select lag_7
from "main"."fct_energy_features"
where lag_7 is null



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-03-11T12:56:16.840066+00:00
-- finished_at: 2026-03-11T12:56:16.847068+00:00
-- elapsed: 7ms
-- outcome: success
-- dialect: duckdb
-- node_id: test.dbt_energy.not_null_fct_energy_features_day_of_week.1ecdd5badf
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.dbt_energy.not_null_fct_energy_features_day_of_week.1ecdd5badf", "profile_name": "dbt_energy", "target_name": "dev"} */
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select day_of_week
from "main"."fct_energy_features"
where day_of_week is null



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-03-11T12:56:16.839689+00:00
-- finished_at: 2026-03-11T12:56:16.847070+00:00
-- elapsed: 7ms
-- outcome: success
-- dialect: duckdb
-- node_id: test.dbt_energy.not_null_fct_energy_features_consumption_gwh.6675a54511
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.dbt_energy.not_null_fct_energy_features_consumption_gwh.6675a54511", "profile_name": "dbt_energy", "target_name": "dev"} */
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select consumption_gwh
from "main"."fct_energy_features"
where consumption_gwh is null



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-03-11T12:56:16.839584+00:00
-- finished_at: 2026-03-11T12:56:16.847157+00:00
-- elapsed: 7ms
-- outcome: success
-- dialect: duckdb
-- node_id: test.dbt_energy.not_null_fct_energy_features_rolling_7_avg.8e5e10f3a8
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.dbt_energy.not_null_fct_energy_features_rolling_7_avg.8e5e10f3a8", "profile_name": "dbt_energy", "target_name": "dev"} */
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select rolling_7_avg
from "main"."fct_energy_features"
where rolling_7_avg is null



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-03-11T12:56:16.839589+00:00
-- finished_at: 2026-03-11T12:56:16.847276+00:00
-- elapsed: 7ms
-- outcome: success
-- dialect: duckdb
-- node_id: test.dbt_energy.not_null_fct_energy_features_is_weekend.c75a191309
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.dbt_energy.not_null_fct_energy_features_is_weekend.c75a191309", "profile_name": "dbt_energy", "target_name": "dev"} */
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select is_weekend
from "main"."fct_energy_features"
where is_weekend is null



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-03-11T12:56:16.839722+00:00
-- finished_at: 2026-03-11T12:56:16.847277+00:00
-- elapsed: 7ms
-- outcome: success
-- dialect: duckdb
-- node_id: test.dbt_energy.not_null_fct_energy_features_lag_1.7e4eeb3883
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.dbt_energy.not_null_fct_energy_features_lag_1.7e4eeb3883", "profile_name": "dbt_energy", "target_name": "dev"} */
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select lag_1
from "main"."fct_energy_features"
where lag_1 is null



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-03-11T12:56:16.839595+00:00
-- finished_at: 2026-03-11T12:56:16.847390+00:00
-- elapsed: 7ms
-- outcome: success
-- dialect: duckdb
-- node_id: test.dbt_energy.not_null_fct_energy_features_date.b3b8a60384
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.dbt_energy.not_null_fct_energy_features_date.b3b8a60384", "profile_name": "dbt_energy", "target_name": "dev"} */
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select date
from "main"."fct_energy_features"
where date is null



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-03-11T12:56:16.841234+00:00
-- finished_at: 2026-03-11T12:56:16.847975+00:00
-- elapsed: 6ms
-- outcome: success
-- dialect: duckdb
-- node_id: test.dbt_energy.not_null_stg_energy_date.2c80140e19
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.dbt_energy.not_null_stg_energy_date.2c80140e19", "profile_name": "dbt_energy", "target_name": "dev"} */
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select date
from "main"."stg_energy"
where date is null



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-03-11T12:56:16.848720+00:00
-- finished_at: 2026-03-11T12:56:16.849081+00:00
-- elapsed: 361us
-- outcome: success
-- dialect: duckdb
-- node_id: test.dbt_energy.not_null_stg_weather_date.2c564dee75
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.dbt_energy.not_null_stg_weather_date.2c564dee75", "profile_name": "dbt_energy", "target_name": "dev"} */
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select date
from "main"."stg_weather"
where date is null



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-03-11T12:56:16.839682+00:00
-- finished_at: 2026-03-11T12:56:16.849634+00:00
-- elapsed: 9ms
-- outcome: success
-- dialect: duckdb
-- node_id: test.dbt_energy.not_null_stg_energy_consumption_gwh.a688875322
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.dbt_energy.not_null_stg_energy_consumption_gwh.a688875322", "profile_name": "dbt_energy", "target_name": "dev"} */
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select consumption_gwh
from "main"."stg_energy"
where consumption_gwh is null



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-03-11T12:56:16.844379+00:00
-- finished_at: 2026-03-11T12:56:16.849664+00:00
-- elapsed: 5ms
-- outcome: success
-- dialect: duckdb
-- node_id: test.dbt_energy.accepted_values_fct_energy_fea_a3901a09c09e989bec4a7c2a82a9e0df.8f46f8072c
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.dbt_energy.accepted_values_fct_energy_fea_a3901a09c09e989bec4a7c2a82a9e0df.8f46f8072c", "profile_name": "dbt_energy", "target_name": "dev"} */
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        day_of_week as value_field,
        count(*) as n_records

    from "main"."fct_energy_features"
    group by day_of_week

)

select *
from all_values
where value_field not in (
    '0','1','2','3','4','5','6'
)



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-03-11T12:56:16.839668+00:00
-- finished_at: 2026-03-11T12:56:16.849773+00:00
-- elapsed: 10ms
-- outcome: success
-- dialect: duckdb
-- node_id: test.dbt_energy.not_null_stg_weather_temperature_c.3a7a09f65f
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.dbt_energy.not_null_stg_weather_temperature_c.3a7a09f65f", "profile_name": "dbt_energy", "target_name": "dev"} */
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select temperature_c
from "main"."stg_weather"
where temperature_c is null



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-03-11T12:56:16.850374+00:00
-- finished_at: 2026-03-11T12:56:16.850732+00:00
-- elapsed: 358us
-- outcome: success
-- dialect: duckdb
-- node_id: test.dbt_energy.accepted_values_fct_energy_features_is_weekend__0__1.e37886f651
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.dbt_energy.accepted_values_fct_energy_features_is_weekend__0__1.e37886f651", "profile_name": "dbt_energy", "target_name": "dev"} */
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        is_weekend as value_field,
        count(*) as n_records

    from "main"."fct_energy_features"
    group by is_weekend

)

select *
from all_values
where value_field not in (
    '0','1'
)



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-03-11T12:56:16.841562+00:00
-- finished_at: 2026-03-11T12:56:16.851891+00:00
-- elapsed: 10ms
-- outcome: success
-- dialect: duckdb
-- node_id: test.dbt_energy.unique_fct_energy_features_date.83575d005f
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.dbt_energy.unique_fct_energy_features_date.83575d005f", "profile_name": "dbt_energy", "target_name": "dev"} */
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    date as unique_field,
    count(*) as n_records

from "main"."fct_energy_features"
where date is not null
group by date
having count(*) > 1



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-03-11T12:56:16.843744+00:00
-- finished_at: 2026-03-11T12:56:16.852323+00:00
-- elapsed: 8ms
-- outcome: success
-- dialect: duckdb
-- node_id: test.dbt_energy.unique_stg_weather_date.4918c96c86
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.dbt_energy.unique_stg_weather_date.4918c96c86", "profile_name": "dbt_energy", "target_name": "dev"} */
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    date as unique_field,
    count(*) as n_records

from "main"."stg_weather"
where date is not null
group by date
having count(*) > 1



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-03-11T12:56:16.851606+00:00
-- finished_at: 2026-03-11T12:56:16.852469+00:00
-- elapsed: 863us
-- outcome: success
-- dialect: duckdb
-- node_id: test.dbt_energy.unique_stg_energy_date.20d227cf9c
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.dbt_energy.unique_stg_energy_date.20d227cf9c", "profile_name": "dbt_energy", "target_name": "dev"} */
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    date as unique_field,
    count(*) as n_records

from "main"."stg_energy"
where date is not null
group by date
having count(*) > 1



  
  
      
    ) dbt_internal_test;
