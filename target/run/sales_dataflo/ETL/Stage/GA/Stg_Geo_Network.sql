
    delete from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Geo_Network
    where (ID) in (
        select (ID)
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Geo_Network__dbt_tmp
    );
    

    insert into DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Geo_Network ("ID", "DATE", "PROFILE", "CONTINENT", "COUNTRY", "CITY", "METRO", "REGION", "NETWORK_LOCATION", "SESSIONS", "USERS", "SOURCE_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS")
    (
        select "ID", "DATE", "PROFILE", "CONTINENT", "COUNTRY", "CITY", "METRO", "REGION", "NETWORK_LOCATION", "SESSIONS", "USERS", "SOURCE_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS"
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Geo_Network__dbt_tmp
    );
