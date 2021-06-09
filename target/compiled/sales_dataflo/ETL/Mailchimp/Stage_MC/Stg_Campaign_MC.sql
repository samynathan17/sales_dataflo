








 



    
      
  select
        md5(cast(
    
    coalesce(cast(id as 
    varchar
), '')

 as 
    varchar
))  AS Campaign_ID,
        ID as Source_ID,
        TYPE,
        CREATE_TIME,
        ARCHIVE_URL,
        LONG_ARCHIVE_URL,
        STATUS,
        SEND_TIME,
        CONTENT_TYPE,
        LIST_ID,
        SEGMENT_TEXT,
        SEGMENT_ID,
        TITLE,
        TO_NAME,
        AUTHENTICATE,
        TIMEWARP,
        SUBJECT_LINE,
        FROM_NAME,
        REPLY_TO,
        USE_CONVERSATION,
        FOLDER_ID,
        AUTO_FOOTER,
        INLINE_CSS,
        AUTO_TWEET,
        FB_COMMENTS,
        TEMPLATE_ID,
        DRAG_AND_DROP,
        CLICKTALE,
        TRACK_HTML_CLICKS,
        TRACK_TEXT_CLICKS,
        TRACK_GOALS,
        TRACK_OPENS,
        TRACK_ECOMM_360,
        GOOGLE_ANALYTICS,
        _FIVETRAN_DELETED,
        WINNING_COMBINATION_ID,
        WINNING_CAMPAIGN_ID,
        WINNER_CRITERIA,
        WAIT_TIME,
        TEST_SIZE,
        _FIVETRAN_SYNCED,
        'MC_ANANDLIVE_16042021' as Source_type,
        'D_CAMPAIGN_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM MC_ANANDLIVE_16042021.CAMPAIGN
         
    
