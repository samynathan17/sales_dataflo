
    delete from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Engagement_Task
    where (Engagement_Task_ID) in (
        select (Engagement_Task_ID)
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Engagement_Task__dbt_tmp
    );
    

    insert into DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Engagement_Task ("ENGAGEMENT_TASK_ID", "ENGAGEMENT_ID", "BODY", "SUBJECT", "STATUS", "FOR_OBJECT_TYPE", "TASK_TYPE", "_FIVETRAN_SYNCED", "SEQUENCE_STEP_ORDER", "COMPLETION_DATE", "SOURCE_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS")
    (
        select "ENGAGEMENT_TASK_ID", "ENGAGEMENT_ID", "BODY", "SUBJECT", "STATUS", "FOR_OBJECT_TYPE", "TASK_TYPE", "_FIVETRAN_SYNCED", "SEQUENCE_STEP_ORDER", "COMPLETION_DATE", "SOURCE_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS"
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Engagement_Task__dbt_tmp
    );
