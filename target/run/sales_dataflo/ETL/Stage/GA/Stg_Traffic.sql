
    delete from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Traffic
    where (ID) in (
        select (ID)
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Traffic__dbt_tmp
    );
    

    insert into DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Traffic ("ID", "DATE", "PROFILE", "PAGE_TITLE", "PAGEVIEWS", "AVG_TIME_ON_PAGE", "PAGE_VALUE", "UNIQUE_PAGEVIEWS", "EXIT_RATE", "ENTRANCES", "USERS", "BOUNCE_RATE", "SOURCE_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS")
    (
        select "ID", "DATE", "PROFILE", "PAGE_TITLE", "PAGEVIEWS", "AVG_TIME_ON_PAGE", "PAGE_VALUE", "UNIQUE_PAGEVIEWS", "EXIT_RATE", "ENTRANCES", "USERS", "BOUNCE_RATE", "SOURCE_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS"
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Traffic__dbt_tmp
    );
