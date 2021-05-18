
    delete from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Fivertan_Audit
    where (Audit_ID) in (
        select (Audit_ID)
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Fivertan_Audit__dbt_tmp
    );
    

    insert into DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Fivertan_Audit ("AUDIT_ID", "SOURCE_ID", "MESSAGE", "UPDATE_STARTED", "UPDATE_ID", "SCHEMA", "DONE", "ROWS_UPDATED_OR_INSERTED", "STATUS", "PROGRESS", "SOURCE_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS")
    (
        select "AUDIT_ID", "SOURCE_ID", "MESSAGE", "UPDATE_STARTED", "UPDATE_ID", "SCHEMA", "DONE", "ROWS_UPDATED_OR_INSERTED", "STATUS", "PROGRESS", "SOURCE_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS"
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Fivertan_Audit__dbt_tmp
    );
