







 



  
 
  select
        md5(cast(
    
    coalesce(cast(ENGAGEMENT_ID as 
    varchar
), '')

 as 
    varchar
))  AS Engagement_Call_ID,
        ENGAGEMENT_ID,
        TO_NUMBER,
        FROM_NUMBER,
        STATUS,
        EXTERNAL_ID,
        DURATION_MILLISECONDS,
        EXTERNAL_ACCOUNT_ID,
        RECORDING_URL,
        BODY,
        DISPOSITION,
        CALLEE_OBJECT_TYPE,
        CALLEE_OBJECT_ID,
        TRANSCRIPTION_ID,
        UNKNOWN_VISITOR_CONVERSATION,
        SOURCE,
        TITLE,
        _FIVETRAN_SYNCED,
        'HS_RKLIVE_01042021' as Source_type,
        'D_COMPANY_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS    
    FROM HS_RKLIVE_01042021.ENGAGEMENT_CALL
     
    
