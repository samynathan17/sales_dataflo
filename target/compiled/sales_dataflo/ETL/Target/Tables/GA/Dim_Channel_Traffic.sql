



WITH source AS
 (
 select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Channel_Traffic
  ),
DIM_CHANNEL_TRAFFIC as (
      select
        ID,
        DATE,
        PROFILE,
        CHANNEL_GROUPING,
        GOAL_VALUE_ALL,
        NEW_USERS,
        SESSIONS,
        AVG_SESSION_DURATION,
        GOAL_COMPLETIONS_ALL,
        PAGEVIEWS_PER_SESSION,
        GOAL_CONVERSION_RATE_ALL,
        USERS,
        BOUNCE_RATE,
        PERCENT_NEW_SESSIONS,
        Source_type,
        'D_CHANNEL_TRAFFIC_DIM_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM source
    )
select * from DIM_CHANNEL_TRAFFIC