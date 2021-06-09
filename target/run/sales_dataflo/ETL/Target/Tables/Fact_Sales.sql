
    delete from DATAFLOTEST_DATABASE.dbt_salesdataflo.Fact_Sales
    where (Report_Date) in (
        select (Report_Date)
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Fact_Sales__dbt_tmp
    );
    

    insert into DATAFLOTEST_DATABASE.dbt_salesdataflo.Fact_Sales ("REPORT_DATE", "ENTITY_ID", "EMPLOYEE_ID", "METRIC_ID", "METRIC_CATEGORY_ID", "TIMEFRAME_TYPE", "AMOUNT", "COUNT", "AVERAGE", "REPORTING_CURRENCY", "DW_SESSION_NM", "DW_INS_UPD_DTS")
    (
        select "REPORT_DATE", "ENTITY_ID", "EMPLOYEE_ID", "METRIC_ID", "METRIC_CATEGORY_ID", "TIMEFRAME_TYPE", "AMOUNT", "COUNT", "AVERAGE", "REPORTING_CURRENCY", "DW_SESSION_NM", "DW_INS_UPD_DTS"
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Fact_Sales__dbt_tmp
    );
