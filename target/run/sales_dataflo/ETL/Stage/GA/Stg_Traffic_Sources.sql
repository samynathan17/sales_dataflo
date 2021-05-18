
    delete from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Traffic_Sources
    where (ID) in (
        select (ID)
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Traffic_Sources__dbt_tmp
    );
    

    insert into DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Traffic_Sources ("ID", "DATE", "PROFILE", "REFERRAL_PATH", "CAMPAIGN", "SOURCE", "MEDIUM", "SOURCE_MEDIUM", "FULL_REFERRER", "ORGANIC_SEARCHES", "SOURCE_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS")
    (
        select "ID", "DATE", "PROFILE", "REFERRAL_PATH", "CAMPAIGN", "SOURCE", "MEDIUM", "SOURCE_MEDIUM", "FULL_REFERRER", "ORGANIC_SEARCHES", "SOURCE_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS"
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Traffic_Sources__dbt_tmp
    );
