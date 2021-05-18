
  create or replace  view DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Platform_Device  as (
    



WITH source AS
 (
 select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Platform_Device
  ),
DIM_PLATFORM_DEVICE as (
      select
        ID,
        DATE,
        PROFILE,
        MOBILE_DEVICE_BRANDING,
        DEVICE_CATEGORY,
        MOBILE_DEVICE_MODEL,
        MOBILE_INPUT_SELECTOR,
        OPERATING_SYSTEM,
        DATA_SOURCE,
        GOAL_VALUE_ALL,
        GOAL_COMPLETIONS_ALL,
        GOAL_STARTS_ALL,
        GOAL_CONVERSION_RATE_ALL,
        GOAL_ABANDONS_ALL,
        GOAL_VALUE_PER_SESSION,
        Source_type,
        'D_PLATFORM_DEVICE_DIM_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM source
    )
select * from DIM_PLATFORM_DEVICE
  );
