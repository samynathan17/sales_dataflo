






 



  
      
  select
        md5(cast(
    
    coalesce(cast(_FIVETRAN_ID as 
    varchar
), '') || '-' || coalesce(cast(DATE as 
    varchar
), '')

 as 
    varchar
))  AS ID,
        DATE,
        PROFILE,
        CAMPAIGN,
        GOAL_VALUE_ALL,
        NEW_USERS,
        SESSIONS,
        GOAL_COMPLETIONS_ALL,
        PAGEVIEWS_PER_SESSION,
        GOAL_CONVERSION_RATE_ALL,
        USERS,
        BOUNCE_RATE,        
        'GA_ANANDLIVE_13042021' as Source_type,
        'D_CAMPAIGN_PERFORMANCE_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM GA_ANANDLIVE_13042021.CAMPAIGN_PERFORMANCE
          
            UNION ALL
         
        


 



  
      
  select
        md5(cast(
    
    coalesce(cast(_FIVETRAN_ID as 
    varchar
), '') || '-' || coalesce(cast(DATE as 
    varchar
), '')

 as 
    varchar
))  AS ID,
        DATE,
        PROFILE,
        CAMPAIGN,
        GOAL_VALUE_ALL,
        NEW_USERS,
        SESSIONS,
        GOAL_COMPLETIONS_ALL,
        PAGEVIEWS_PER_SESSION,
        GOAL_CONVERSION_RATE_ALL,
        USERS,
        BOUNCE_RATE,        
        'GA_RKLIVE_01042021' as Source_type,
        'D_CAMPAIGN_PERFORMANCE_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM GA_RKLIVE_01042021.CAMPAIGN_PERFORMANCE
           
        
