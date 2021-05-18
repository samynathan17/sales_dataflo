







 



  
      
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
        SESSION_DURATION_BUCKET,
        USER_TYPE,
        HITS,
        SESSIONS,
        SESSIONS_PER_USER,
        AVG_SESSION_DURATION,
        BOUNCES,
        SESSION_DURATION,
        BOUNCE_RATE,

        
        'GA_DATAFLO_22042021' as Source_type,
        'D_SESSION_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM GA_DATAFLO_22042021.SESSION
           
        
