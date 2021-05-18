
    delete from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Adwords_Keyword
    where (ID) in (
        select (ID)
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Adwords_Keyword__dbt_tmp
    );
    

    insert into DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Adwords_Keyword ("ID", "DATE", "PROFILE", "KEYWORD", "GOAL_VALUE_ALL", "SESSIONS", "GOAL_COMPLETIONS_ALL", "PAGEVIEWS_PER_SESSION", "GOAL_CONVERSION_RATE_ALL", "USERS", "BOUNCE_RATE", "AD_CLICKS", "AD_COST", "CPC", "SOURCE_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS")
    (
        select "ID", "DATE", "PROFILE", "KEYWORD", "GOAL_VALUE_ALL", "SESSIONS", "GOAL_COMPLETIONS_ALL", "PAGEVIEWS_PER_SESSION", "GOAL_CONVERSION_RATE_ALL", "USERS", "BOUNCE_RATE", "AD_CLICKS", "AD_COST", "CPC", "SOURCE_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS"
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Adwords_Keyword__dbt_tmp
    );
