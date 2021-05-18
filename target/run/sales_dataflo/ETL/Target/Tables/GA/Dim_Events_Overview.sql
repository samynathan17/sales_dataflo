
  create or replace  view DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Events_Overview  as (
    



WITH source AS
 (
 select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Events_Overview
  ),
DIM_EVENTS_OVERVIEW as (
      select
        ID,
        DATE,
        PROFILE,
        EVENT_CATEGORY,
        EVENT_VALUE,
        TOTAL_EVENTS,
        SESSIONS_WITH_EVENT,
        EVENTS_PER_SESSION_WITH_EVENT,
        AVG_EVENT_VALUE,
        UNIQUE_EVENTS,
        Source_type,
        'D_EVENTS_OVERVIEW_DIM_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM source
    )
select * from DIM_EVENTS_OVERVIEW
  );
