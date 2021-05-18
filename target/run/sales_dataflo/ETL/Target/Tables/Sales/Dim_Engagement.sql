
    delete from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Engagement
    where (ENGAGEMENT_ID) in (
        select (ENGAGEMENT_ID)
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Engagement__dbt_tmp
    );
    

    insert into DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Engagement ("ENGAGEMENT_ID", "SOURCE_ID", "SOURCE_TYPE", "EMPLOYEE_ID", "TYPE", "INITIAL_CREATE_DT", "LAST_UPDATED", "IS_ACTIVE", "DW_SESSION_NM", "DW_INS_UPD_DTS")
    (
        select "ENGAGEMENT_ID", "SOURCE_ID", "SOURCE_TYPE", "EMPLOYEE_ID", "TYPE", "INITIAL_CREATE_DT", "LAST_UPDATED", "IS_ACTIVE", "DW_SESSION_NM", "DW_INS_UPD_DTS"
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Engagement__dbt_tmp
    );
