
    delete from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Session
    where (ID) in (
        select (ID)
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Session__dbt_tmp
    );
    

    insert into DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Session ("ID", "DATE", "PROFILE", "SESSION_DURATION_BUCKET", "USER_TYPE", "HITS", "SESSIONS", "SESSIONS_PER_USER", "AVG_SESSION_DURATION", "BOUNCES", "SESSION_DURATION", "BOUNCE_RATE", "SOURCE_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS")
    (
        select "ID", "DATE", "PROFILE", "SESSION_DURATION_BUCKET", "USER_TYPE", "HITS", "SESSIONS", "SESSIONS_PER_USER", "AVG_SESSION_DURATION", "BOUNCES", "SESSION_DURATION", "BOUNCE_RATE", "SOURCE_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS"
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Session__dbt_tmp
    );
