
    delete from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Page
    where (Page_Rept_ID) in (
        select (Page_Rept_ID)
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Page__dbt_tmp
    );
    

    insert into DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Page ("PAGE_REPT_ID", "COUNTRY", "DATE_DAY", "DEVICE", "PAGE", "SEARCH_TYPE", "SITE", "CLICKS", "IMPRESSIONS", "CTR", "POSITION", "_FIVETRAN_SYNCED", "PLATFORM", "DW_SESSION_NM", "DW_INS_UPD_DTS")
    (
        select "PAGE_REPT_ID", "COUNTRY", "DATE_DAY", "DEVICE", "PAGE", "SEARCH_TYPE", "SITE", "CLICKS", "IMPRESSIONS", "CTR", "POSITION", "_FIVETRAN_SYNCED", "PLATFORM", "DW_SESSION_NM", "DW_INS_UPD_DTS"
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Page__dbt_tmp
    );
