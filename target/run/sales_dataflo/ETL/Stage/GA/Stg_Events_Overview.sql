
    delete from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Events_Overview
    where (ID) in (
        select (ID)
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Events_Overview__dbt_tmp
    );
    

    insert into DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Events_Overview ("ID", "DATE", "PROFILE", "EVENT_CATEGORY", "EVENT_VALUE", "TOTAL_EVENTS", "SESSIONS_WITH_EVENT", "EVENTS_PER_SESSION_WITH_EVENT", "AVG_EVENT_VALUE", "UNIQUE_EVENTS", "SOURCE_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS")
    (
        select "ID", "DATE", "PROFILE", "EVENT_CATEGORY", "EVENT_VALUE", "TOTAL_EVENTS", "SESSIONS_WITH_EVENT", "EVENTS_PER_SESSION_WITH_EVENT", "AVG_EVENT_VALUE", "UNIQUE_EVENTS", "SOURCE_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS"
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Events_Overview__dbt_tmp
    );
