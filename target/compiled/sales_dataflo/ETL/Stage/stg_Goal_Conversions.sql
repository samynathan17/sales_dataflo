






  
      
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
        GOAL_COMPLETION_LOCATION,
        GOAL_PREVIOUS_STEP_1,
        GOAL_PREVIOUS_STEP_2,
        GOAL_PREVIOUS_STEP_3,
        GOAL_VALUE_ALL,
        GOAL_COMPLETIONS_ALL,
        GOAL_STARTS_ALL,
        GOAL_CONVERSION_RATE_ALL,
        GOAL_ABANDON_RATE_ALL,
        GOAL_ABANDONS_ALL,
        GOAL_VALUE_PER_SESSION,

        
        'GA_DATAFLO_01022021' as Source_type,
        'D_GOAL_CONVERSIONS_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM GA_DATAFLO_01022021.GOAL_CONVERSIONS
          
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
        GOAL_COMPLETION_LOCATION,
        GOAL_PREVIOUS_STEP_1,
        GOAL_PREVIOUS_STEP_2,
        GOAL_PREVIOUS_STEP_3,
        GOAL_VALUE_ALL,
        GOAL_COMPLETIONS_ALL,
        GOAL_STARTS_ALL,
        GOAL_CONVERSION_RATE_ALL,
        GOAL_ABANDON_RATE_ALL,
        GOAL_ABANDONS_ALL,
        GOAL_VALUE_PER_SESSION,

        
        'GA_ANAND_01022021' as Source_type,
        'D_GOAL_CONVERSIONS_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM GA_ANAND_01022021.GOAL_CONVERSIONS
                  
    
