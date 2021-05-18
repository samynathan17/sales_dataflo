
    delete from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Account
    where (Account_ID) in (
        select (Account_ID)
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Account__dbt_tmp
    );
    

    insert into DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Account ("ACCOUNT_ID", "ACCOUNT_NAME", "ACCOUNT_TYPE", "SOURCE_ID", "ACTIVE_FLAG", "INDUSTRY", "ANNUAL_REVENUE", "EMPLOYEE_ID", "INITIAL_CREATE_DT", "SOURCE_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS")
    (
        select "ACCOUNT_ID", "ACCOUNT_NAME", "ACCOUNT_TYPE", "SOURCE_ID", "ACTIVE_FLAG", "INDUSTRY", "ANNUAL_REVENUE", "EMPLOYEE_ID", "INITIAL_CREATE_DT", "SOURCE_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS"
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Account__dbt_tmp
    );
