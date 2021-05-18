
    delete from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Engagement_Email
    where (Engagement_Email_ID) in (
        select (Engagement_Email_ID)
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Engagement_Email__dbt_tmp
    );
    

    insert into DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Engagement_Email ("ENGAGEMENT_EMAIL_ID", "ENGAGEMENT_ID", "FROM_EMAIL", "FROM_FIRST_NAME", "FROM_LAST_NAME", "SUBJECT", "HTML", "TEXT", "TRACKER_KEY", "MESSAGE_ID", "THREAD_ID", "STATUS", "SENT_VIA", "LOGGED_FROM", "ERROR_MESSAGE", "FACSIMILE_SEND_ID", "POST_SEND_STATUS", "MEDIA_PROCESSING_STATUS", "ATTACHED_VIDEO_OPENED", "ATTACHED_VIDEO_WATCHED", "ATTACHED_VIDEO_ID", "RECIPIENT_DROP_REASONS", "VALIDATION_SKIPPED", "EMAIL_SEND_EVENT_ID_CREATED", "EMAIL_SEND_EVENT_ID_ID", "PENDING_INLINE_IMAGE_IDS", "_FIVETRAN_SYNCED", "SOURCE_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS")
    (
        select "ENGAGEMENT_EMAIL_ID", "ENGAGEMENT_ID", "FROM_EMAIL", "FROM_FIRST_NAME", "FROM_LAST_NAME", "SUBJECT", "HTML", "TEXT", "TRACKER_KEY", "MESSAGE_ID", "THREAD_ID", "STATUS", "SENT_VIA", "LOGGED_FROM", "ERROR_MESSAGE", "FACSIMILE_SEND_ID", "POST_SEND_STATUS", "MEDIA_PROCESSING_STATUS", "ATTACHED_VIDEO_OPENED", "ATTACHED_VIDEO_WATCHED", "ATTACHED_VIDEO_ID", "RECIPIENT_DROP_REASONS", "VALIDATION_SKIPPED", "EMAIL_SEND_EVENT_ID_CREATED", "EMAIL_SEND_EVENT_ID_ID", "PENDING_INLINE_IMAGE_IDS", "_FIVETRAN_SYNCED", "SOURCE_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS"
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Engagement_Email__dbt_tmp
    );
