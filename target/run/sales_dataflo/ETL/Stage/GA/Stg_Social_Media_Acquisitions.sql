
    delete from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Social_Media_Acquisitions
    where (ID) in (
        select (ID)
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Social_Media_Acquisitions__dbt_tmp
    );
    

    insert into DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Social_Media_Acquisitions ("ID", "DATE", "PROFILE", "SOCIAL_NETWORK", "SESSIONS", "NEW_USERS", "AVG_SESSION_DURATION", "TRANSACTION_REVENUE", "PAGEVIEWS_PER_SESSION", "TRANSACTIONS", "BOUNCE_RATE", "PAGEVIEWS", "PERCENT_NEW_SESSIONS", "TRANSACTIONS_PER_SESSION", "SOURCE_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS")
    (
        select "ID", "DATE", "PROFILE", "SOCIAL_NETWORK", "SESSIONS", "NEW_USERS", "AVG_SESSION_DURATION", "TRANSACTION_REVENUE", "PAGEVIEWS_PER_SESSION", "TRANSACTIONS", "BOUNCE_RATE", "PAGEVIEWS", "PERCENT_NEW_SESSIONS", "TRANSACTIONS_PER_SESSION", "SOURCE_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS"
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Social_Media_Acquisitions__dbt_tmp
    );
