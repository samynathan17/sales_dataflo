
    delete from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Engagement
    where (Engagement_ID) in (
        select (Engagement_ID)
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Engagement__dbt_tmp
    );
    

    insert into DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Engagement ("ENGAGEMENT_ID", "SOURCE_ID", "PORTAL_ID", "ACTIVE", "OWNER_ID", "TYPE", "ACTIVITY_TYPE", "CREATED_AT", "LAST_UPDATED", "TIMESTAMP", "_FIVETRAN_SYNCED", "SOURCE_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS")
    (
        select "ENGAGEMENT_ID", "SOURCE_ID", "PORTAL_ID", "ACTIVE", "OWNER_ID", "TYPE", "ACTIVITY_TYPE", "CREATED_AT", "LAST_UPDATED", "TIMESTAMP", "_FIVETRAN_SYNCED", "SOURCE_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS"
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Engagement__dbt_tmp
    );
