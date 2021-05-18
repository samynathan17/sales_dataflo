
    delete from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Page_Tracking
    where (ID) in (
        select (ID)
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Page_Tracking__dbt_tmp
    );
    

    insert into DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Page_Tracking ("ID", "DATE", "PROFILE", "PAGE_TITLE", "LANDING_PAGE_PATH", "PAGE_PATH", "EXIT_PAGE_PATH", "PAGE_VALUE", "EXIT_RATE", "TIME_ON_PAGE", "PAGEVIEWS_PER_SESSION", "UNIQUE_PAGEVIEWS", "ENTRANCE_RATE", "SOURCE_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS")
    (
        select "ID", "DATE", "PROFILE", "PAGE_TITLE", "LANDING_PAGE_PATH", "PAGE_PATH", "EXIT_PAGE_PATH", "PAGE_VALUE", "EXIT_RATE", "TIME_ON_PAGE", "PAGEVIEWS_PER_SESSION", "UNIQUE_PAGEVIEWS", "ENTRANCE_RATE", "SOURCE_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS"
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Page_Tracking__dbt_tmp
    );
