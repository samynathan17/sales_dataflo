
  create or replace  view DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Ttraffic  as (
    



WITH source AS
 (
 select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Traffic
  ),
DIM_TRAFFIC as (
      select
        ID,
        DATE,
        PROFILE,
        PAGE_TITLE,
        PAGEVIEWS,
        AVG_TIME_ON_PAGE,
        PAGE_VALUE,
        UNIQUE_PAGEVIEWS,
        EXIT_RATE,
        ENTRANCES,
        USERS,
        BOUNCE_RATE, 
        Source_type,
        'D_TRAFFIC_DIM_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM source
    )
select * from DIM_TRAFFIC
  );
