
    delete from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Keyword_Site
    where (Site_Rept_ID) in (
        select (Site_Rept_ID)
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Keyword_Site__dbt_tmp
    );
    

    insert into DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Keyword_Site ("SITE_REPT_ID", "COUNTRY", "DATE_DAY", "DEVICE", "KEYWORD", "SEARCH_TYPE", "SITE", "CLICKS", "IMPRESSIONS", "CTR", "POSITION", "_FIVETRAN_SYNCED", "PLATFORM", "DW_SESSION_NM", "DW_INS_UPD_DTS")
    (
        select "SITE_REPT_ID", "COUNTRY", "DATE_DAY", "DEVICE", "KEYWORD", "SEARCH_TYPE", "SITE", "CLICKS", "IMPRESSIONS", "CTR", "POSITION", "_FIVETRAN_SYNCED", "PLATFORM", "DW_SESSION_NM", "DW_INS_UPD_DTS"
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Keyword_Site__dbt_tmp
    );
