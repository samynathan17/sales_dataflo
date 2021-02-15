






     
  select
        md5(cast(
    
    coalesce(cast(STAGE_ID as 
    varchar
), '')

 as 
    varchar
))  AS PIPELINE_STAGE_ID,
        STAGE_ID as Source_STAGE_ID,
        PIPELINE_ID,
        LABEL,
        PROBABILITY,
        ACTIVE,
        DISPLAY_ORDER,
        CLOSED_WON, 
        'HS_TESTUSER_09012021' as Source_type,
        'D_DEAL_PIPELINE_STAGE_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS    
    FROM HS_TESTUSER_09012021.DEAL_PIPELINE_STAGE
    
    
