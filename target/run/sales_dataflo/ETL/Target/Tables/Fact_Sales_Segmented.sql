
    delete from DATAFLOTEST_DATABASE.dbt_salesdataflo.Fact_Sales_Segmented
    where (Report_Date1) in (
        select (Report_Date1)
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Fact_Sales_Segmented__dbt_tmp
    );
    

    insert into DATAFLOTEST_DATABASE.dbt_salesdataflo.Fact_Sales_Segmented ("REPORT_DATE", "ENTITY_ID", "EMPLOYEE_ID", "METRIC_ID", "METRIC_CATEGORY_ID", "SEGMENT_NAME", "TIMEFRAME_TYPE", "AMOUNT", "COUNT", "AVERAGE", "REPORTING_CURRENCY", "DW_SESSION_NM", "DW_INS_UPD_DTS")
    (
        select "REPORT_DATE", "ENTITY_ID", "EMPLOYEE_ID", "METRIC_ID", "METRIC_CATEGORY_ID", "SEGMENT_NAME", "TIMEFRAME_TYPE", "AMOUNT", "COUNT", "AVERAGE", "REPORTING_CURRENCY", "DW_SESSION_NM", "DW_INS_UPD_DTS"
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Fact_Sales_Segmented__dbt_tmp
    );
