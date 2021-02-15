






  
      
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
        OPERATING_SYSTEM,
        BROWSER,
        GOAL_VALUE_ALL,
        NEW_USERS,
        SESSIONS,
        AVG_SESSION_DURATION,
        GOAL_COMPLETIONS_ALL,
        PAGEVIEWS_PER_SESSION,
        GOAL_CONVERSION_RATE_ALL,
        USERS,
        BOUNCE_RATE,
   
        'GA_DATAFLO_01022021' as Source_type,
        'D_BROWSER_AND_OPERATING_SYSTEM_OVERVIEW_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM GA_DATAFLO_01022021.BROWSER_AND_OPERATING_SYSTEM_OVERVIEW
          
            UNION ALL
                
    


  
      
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
        OPERATING_SYSTEM,
        BROWSER,
        GOAL_VALUE_ALL,
        NEW_USERS,
        SESSIONS,
        AVG_SESSION_DURATION,
        GOAL_COMPLETIONS_ALL,
        PAGEVIEWS_PER_SESSION,
        GOAL_CONVERSION_RATE_ALL,
        USERS,
        BOUNCE_RATE,
   
        'GA_ANAND_01022021' as Source_type,
        'D_BROWSER_AND_OPERATING_SYSTEM_OVERVIEW_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM GA_ANAND_01022021.BROWSER_AND_OPERATING_SYSTEM_OVERVIEW
                  
    
