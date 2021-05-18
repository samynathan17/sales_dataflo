







 



  
 
  select
        md5(cast(
    
    coalesce(cast(ENGAGEMENT_ID as 
    varchar
), '')

 as 
    varchar
))  AS Engagement_Email_ID,
        ENGAGEMENT_ID,
        FROM_EMAIL,
        FROM_FIRST_NAME,
        FROM_LAST_NAME,
        SUBJECT,
        HTML,
        TEXT,
        TRACKER_KEY,
        MESSAGE_ID,
        THREAD_ID,
        STATUS,
        SENT_VIA,
        LOGGED_FROM,
        ERROR_MESSAGE,
        FACSIMILE_SEND_ID,
        POST_SEND_STATUS,
        MEDIA_PROCESSING_STATUS,
        ATTACHED_VIDEO_OPENED,
        ATTACHED_VIDEO_WATCHED,
        ATTACHED_VIDEO_ID,
        RECIPIENT_DROP_REASONS,
        VALIDATION_SKIPPED,
        EMAIL_SEND_EVENT_ID_CREATED,
        EMAIL_SEND_EVENT_ID_ID,
        PENDING_INLINE_IMAGE_IDS,
        _FIVETRAN_SYNCED,
        'HS_RKLIVE_01042021' as Source_type,
        'D_COMPANY_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS    
    FROM HS_RKLIVE_01042021.ENGAGEMENT_EMAIL
     
    
