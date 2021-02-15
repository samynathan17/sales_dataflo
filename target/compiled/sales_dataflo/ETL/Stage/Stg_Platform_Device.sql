






  
      
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
        MOBILE_DEVICE_BRANDING,
        DEVICE_CATEGORY,
        MOBILE_DEVICE_MODEL,
        MOBILE_INPUT_SELECTOR,
        OPERATING_SYSTEM,
        DATA_SOURCE,
        GOAL_VALUE_ALL,
        GOAL_COMPLETIONS_ALL,
        GOAL_STARTS_ALL,
        GOAL_CONVERSION_RATE_ALL,
        GOAL_ABANDONS_ALL,
        GOAL_VALUE_PER_SESSION,

        
        'GA_DATAFLO_01022021' as Source_type,
        'D_PLATFORM_DEVICE_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM GA_DATAFLO_01022021.PLATFORM_DEVICE
          
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
        MOBILE_DEVICE_BRANDING,
        DEVICE_CATEGORY,
        MOBILE_DEVICE_MODEL,
        MOBILE_INPUT_SELECTOR,
        OPERATING_SYSTEM,
        DATA_SOURCE,
        GOAL_VALUE_ALL,
        GOAL_COMPLETIONS_ALL,
        GOAL_STARTS_ALL,
        GOAL_CONVERSION_RATE_ALL,
        GOAL_ABANDONS_ALL,
        GOAL_VALUE_PER_SESSION,

        
        'GA_ANAND_01022021' as Source_type,
        'D_PLATFORM_DEVICE_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM GA_ANAND_01022021.PLATFORM_DEVICE
                  
    