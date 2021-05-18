







 



  
 
  select
        md5(cast(
    
    coalesce(cast(ENGAGEMENT_ID as 
    varchar
), '')

 as 
    varchar
))  AS ENGAGEMENT_TASK_ID,
        ENGAGEMENT_ID,
        BODY,
        SUBJECT,
        STATUS,
        FOR_OBJECT_TYPE,
        TASK_TYPE,
        _FIVETRAN_SYNCED,
        SEQUENCE_STEP_ORDER,
        COMPLETION_DATE,
        'HS_RKLIVE_01042021' as Source_type,
        'D_COMPANY_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS    
    FROM HS_RKLIVE_01042021.ENGAGEMENT_TASK
     
    
