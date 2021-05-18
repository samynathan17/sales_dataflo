
    delete from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Owner
    where (OWNER_ID) in (
        select (OWNER_ID)
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Owner__dbt_tmp
    );
    

    insert into DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Owner ("OWNER_ID", "SOURCE_OWNER_ID", "PORTAL_ID", "TYPE", "FIRST_NAME", "LAST_NAME", "EMAIL", "CREATED_AT", "UPDATED_AT", "IS_ACTIVE", "ACTIVE_USER_ID", "USER_ID_INCLUDING_INACTIVE", "CUSTOMER_TEXT_1", "CUSTOMER_TEXT_2", "CUSTOMER_TEXT_3", "CUSTOMER_TEXT_4", "CUSTOMER_TEXT_5", "CUSTOMER_TEXT_6", "CUSTOMER_NUMBER_1", "CUSTOMER_NUMBER_2", "CUSTOMER_NUMBER_3", "CUSTOMER_DATE_1", "CUSTOMER_DATE_2", "CUSTOMER_DATE_3", "SOURCE_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS")
    (
        select "OWNER_ID", "SOURCE_OWNER_ID", "PORTAL_ID", "TYPE", "FIRST_NAME", "LAST_NAME", "EMAIL", "CREATED_AT", "UPDATED_AT", "IS_ACTIVE", "ACTIVE_USER_ID", "USER_ID_INCLUDING_INACTIVE", "CUSTOMER_TEXT_1", "CUSTOMER_TEXT_2", "CUSTOMER_TEXT_3", "CUSTOMER_TEXT_4", "CUSTOMER_TEXT_5", "CUSTOMER_TEXT_6", "CUSTOMER_NUMBER_1", "CUSTOMER_NUMBER_2", "CUSTOMER_NUMBER_3", "CUSTOMER_DATE_1", "CUSTOMER_DATE_2", "CUSTOMER_DATE_3", "SOURCE_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS"
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Owner__dbt_tmp
    );
