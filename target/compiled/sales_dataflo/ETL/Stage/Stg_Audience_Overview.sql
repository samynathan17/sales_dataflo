






  
      
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
        NEW_USERS,
        SESSIONS,
        SESSIONS_PER_USER,
        PAGEVIEWS,
        AVG_SESSION_DURATION,
        PAGEVIEWS_PER_SESSION,
        USERS,
        BOUNCE_RATE,
     
        'GA_DATAFLO_11022021' as Source_type,
        'D_AUDIENCE_OVERVIEW_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM GA_DATAFLO_11022021.AUDIENCE_OVERVIEW
                  
    
