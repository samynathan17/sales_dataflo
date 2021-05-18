
    delete from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Balance_Transaction
    where (BALANCE_TRANSACTION_ID) in (
        select (BALANCE_TRANSACTION_ID)
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Balance_Transaction__dbt_tmp
    );
    

    insert into DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Balance_Transaction ("BALANCE_TRANSACTION_ID", "SOURCE_ID", "CONNECTED_ACCOUNT_ID", "AMOUNT", "AVAILABLE_ON", "CREATED", "CURRENCY", "DESCRIPTION", "EXCHANGE_RATE", "FEE", "NET", "SOURCE", "STATUS", "TYPE", "PAYOUT_ID", "_FIVETRAN_SYNCED", "ENTITY_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS")
    (
        select "BALANCE_TRANSACTION_ID", "SOURCE_ID", "CONNECTED_ACCOUNT_ID", "AMOUNT", "AVAILABLE_ON", "CREATED", "CURRENCY", "DESCRIPTION", "EXCHANGE_RATE", "FEE", "NET", "SOURCE", "STATUS", "TYPE", "PAYOUT_ID", "_FIVETRAN_SYNCED", "ENTITY_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS"
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Balance_Transaction__dbt_tmp
    );
