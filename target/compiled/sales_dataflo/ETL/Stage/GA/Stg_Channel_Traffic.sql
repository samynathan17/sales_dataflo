






 



  
      
  select
        md5(cast(
    
    coalesce(cast(_FIVETRAN_ID as 
    varchar
), '') || '-' || coalesce(cast(PROFILE as 
    varchar
), '') || '-' || coalesce(cast(DATE as 
    varchar
), '')

 as 
    varchar
))  AS ID,
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
        'GA_DATAFLO_22042021' as Source_type,
        'D_CHANNEL_TRAFFIC_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM GA_DATAFLO_22042021.CHANNEL_TRAFFIC 
           
        
