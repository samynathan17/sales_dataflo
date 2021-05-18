







 



  
 
  select
        md5(cast(
    
    coalesce(cast(ENGAGEMENT_ID as 
    varchar
), '')

 as 
    varchar
))  AS ENGAGEMENT_Meeting_ID,
        ENGAGEMENT_ID,
        BODY,
        START_TIME,
        END_TIME,
        TITLE,
        EXTERNAL_URL,
        SOURCE,
        CREATED_FROM_LINK_ID,
        SOURCE_ID,
        WEB_CONFERENCE_MEETING_ID,
        MEETING_OUTCOME,
        PRE_MEETING_PROSPECT_REMINDERS,
        _FIVETRAN_SYNCED,
        'HS_RKLIVE_01042021' as Source_type,
        'D_COMPANY_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS    
    FROM HS_RKLIVE_01042021.ENGAGEMENT_MEETING
     
    
