






   
    
  select
        md5(cast(
    
    coalesce(cast(_FIVETRAN_ID as 
    varchar
), '')

 as 
    varchar
))  AS ID,
        DATE,
        PROFILE,
        EVENT_CATEGORY,
        EVENT_VALUE,
        TOTAL_EVENTS,
        SESSIONS_WITH_EVENT,
        EVENTS_PER_SESSION_WITH_EVENT,
        AVG_EVENT_VALUE,
        UNIQUE_EVENTS,
        'GA_DATAFLO_11022021' as Source_type,
        'D_EVENTS_OVERVIEW_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM GA_DATAFLO_11022021.EVENTS_OVERVIEW
        
    
