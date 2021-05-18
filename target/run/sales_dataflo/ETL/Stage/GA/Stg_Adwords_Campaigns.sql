
    delete from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Adwords_Campaigns
    where (ID) in (
        select (ID)
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Adwords_Campaigns__dbt_tmp
    );
    

    insert into DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Adwords_Campaigns ("ID", "DATE", "PROFILE", "ADWORDS_CAMPAIGN_ID", "GOAL_VALUE_ALL", "SESSIONS", "GOAL_COMPLETIONS_ALL", "GOAL_CONVERSION_RATE_ALL", "USERS", "BOUNCE_RATE", "AD_CLICKS", "AD_COST", "CPC", "SOURCE_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS")
    (
        select "ID", "DATE", "PROFILE", "ADWORDS_CAMPAIGN_ID", "GOAL_VALUE_ALL", "SESSIONS", "GOAL_COMPLETIONS_ALL", "GOAL_CONVERSION_RATE_ALL", "USERS", "BOUNCE_RATE", "AD_CLICKS", "AD_COST", "CPC", "SOURCE_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS"
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Adwords_Campaigns__dbt_tmp
    );
