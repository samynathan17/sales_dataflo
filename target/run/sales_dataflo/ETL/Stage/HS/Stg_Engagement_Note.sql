
    delete from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Engagement_Note
    where (Engagement_Note_ID) in (
        select (Engagement_Note_ID)
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Engagement_Note__dbt_tmp
    );
    

    insert into DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Engagement_Note ("ENGAGEMENT_NOTE_ID", "ENGAGEMENT_ID", "BODY", "_FIVETRAN_SYNCED", "SOURCE_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS")
    (
        select "ENGAGEMENT_NOTE_ID", "ENGAGEMENT_ID", "BODY", "_FIVETRAN_SYNCED", "SOURCE_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS"
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Engagement_Note__dbt_tmp
    );
