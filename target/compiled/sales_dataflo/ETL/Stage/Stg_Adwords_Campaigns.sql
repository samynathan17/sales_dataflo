






  
      
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
        ADWORDS_CAMPAIGN_ID,
        GOAL_VALUE_ALL,
        SESSIONS,
        GOAL_COMPLETIONS_ALL,
        GOAL_CONVERSION_RATE_ALL,
        USERS,
        BOUNCE_RATE,
        AD_CLICKS,
        AD_COST,
        CPC,
        
        'GA_DATAFLO_01022021' as Source_type,
        'D_ADWORDS_CAMPAIGNS_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM GA_DATAFLO_01022021.ADWORDS_CAMPAIGNS
          
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
        ADWORDS_CAMPAIGN_ID,
        GOAL_VALUE_ALL,
        SESSIONS,
        GOAL_COMPLETIONS_ALL,
        GOAL_CONVERSION_RATE_ALL,
        USERS,
        BOUNCE_RATE,
        AD_CLICKS,
        AD_COST,
        CPC,
        
        'GA_ANAND_01022021' as Source_type,
        'D_ADWORDS_CAMPAIGNS_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM GA_ANAND_01022021.ADWORDS_CAMPAIGNS
                  
    
