






 



  
      
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
        PAGE_TITLE,
        PAGEVIEWS,
        AVG_TIME_ON_PAGE,
        PAGE_VALUE,
        UNIQUE_PAGEVIEWS,
        EXIT_RATE,
        ENTRANCES,
        USERS,
        BOUNCE_RATE,        
        'GA_DATAFLO_22042021' as Source_type,
        'D_TRAFFIC_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM GA_DATAFLO_22042021.traffic
            
        
