
    delete from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Browser_And_Operating_System_Overview
    where (ID) in (
        select (ID)
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Browser_And_Operating_System_Overview__dbt_tmp
    );
    

    insert into DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Browser_And_Operating_System_Overview ("ID", "DATE", "PROFILE", "OPERATING_SYSTEM", "BROWSER", "GOAL_VALUE_ALL", "NEW_USERS", "SESSIONS", "AVG_SESSION_DURATION", "GOAL_COMPLETIONS_ALL", "PAGEVIEWS_PER_SESSION", "GOAL_CONVERSION_RATE_ALL", "USERS", "BOUNCE_RATE", "SOURCE_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS")
    (
        select "ID", "DATE", "PROFILE", "OPERATING_SYSTEM", "BROWSER", "GOAL_VALUE_ALL", "NEW_USERS", "SESSIONS", "AVG_SESSION_DURATION", "GOAL_COMPLETIONS_ALL", "PAGEVIEWS_PER_SESSION", "GOAL_CONVERSION_RATE_ALL", "USERS", "BOUNCE_RATE", "SOURCE_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS"
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Browser_And_Operating_System_Overview__dbt_tmp
    );
