
    delete from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Fee
    where (FEE_ID) in (
        select (FEE_ID)
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Fee__dbt_tmp
    );
    

    insert into DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Fee ("FEE_ID", "BALANCE_TRANSACTION_ID", "INDEX", "CONNECTED_ACCOUNT_ID", "AMOUNT", "APPLICATION", "CURRENCY", "DESCRIPTION", "TYPE", "_FIVETRAN_SYNCED", "ENTITY_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS")
    (
        select "FEE_ID", "BALANCE_TRANSACTION_ID", "INDEX", "CONNECTED_ACCOUNT_ID", "AMOUNT", "APPLICATION", "CURRENCY", "DESCRIPTION", "TYPE", "_FIVETRAN_SYNCED", "ENTITY_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS"
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Fee__dbt_tmp
    );
