
    delete from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Opportunity
    where (opportunity_id) in (
        select (opportunity_id)
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Opportunity__dbt_tmp
    );
    

    insert into DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Opportunity ("OPPORTUNITY_ID", "SOURCE_ID", "IS_DELETED", "ACCOUNT_ID", "NAME", "DESCRIPTION", "STAGE_NAME", "AMOUNT", "CLOSE_DATE", "TYPE", "NEXT_STEP", "LEAD_SOURCE", "IS_CLOSED", "IS_WON", "CURRENCY_ISO_CODE", "REPORTING_CURRENCY", "FORECAST_CATEGORY", "FORECAST_CATEGORY_NAME", "HAS_OPPORTUNITY_LINE_ITEM", "OWNER_ID", "CREATED_DATE", "CREATED_BY_ID", "LAST_MODIFIED_DATE", "LAST_MODIFIED_BY_ID", "SYSTEM_MODSTAMP", "LAST_ACTIVITY_DATE", "FISCAL_QUARTER", "FISCAL_YEAR", "FISCAL", "LAST_VIEWED_DATE", "LAST_REFERENCED_DATE", "HAS_OPEN_ACTIVITY", "HAS_OVERDUE_TASK", "CONTACT_ID", "CUSTOMER_TEXT_1", "CUSTOMER_TEXT_2", "CUSTOMER_TEXT_3", "CUSTOMER_TEXT_4", "CUSTOMER_TEXT_5", "CUSTOMER_TEXT_6", "CUSTOMER_NUMBER_1", "CUSTOMER_NUMBER_2", "CUSTOMER_NUMBER_3", "CUSTOMER_DATE_1", "CUSTOMER_DATE_2", "CUSTOMER_DATE_3", "SOURCE_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS")
    (
        select "OPPORTUNITY_ID", "SOURCE_ID", "IS_DELETED", "ACCOUNT_ID", "NAME", "DESCRIPTION", "STAGE_NAME", "AMOUNT", "CLOSE_DATE", "TYPE", "NEXT_STEP", "LEAD_SOURCE", "IS_CLOSED", "IS_WON", "CURRENCY_ISO_CODE", "REPORTING_CURRENCY", "FORECAST_CATEGORY", "FORECAST_CATEGORY_NAME", "HAS_OPPORTUNITY_LINE_ITEM", "OWNER_ID", "CREATED_DATE", "CREATED_BY_ID", "LAST_MODIFIED_DATE", "LAST_MODIFIED_BY_ID", "SYSTEM_MODSTAMP", "LAST_ACTIVITY_DATE", "FISCAL_QUARTER", "FISCAL_YEAR", "FISCAL", "LAST_VIEWED_DATE", "LAST_REFERENCED_DATE", "HAS_OPEN_ACTIVITY", "HAS_OVERDUE_TASK", "CONTACT_ID", "CUSTOMER_TEXT_1", "CUSTOMER_TEXT_2", "CUSTOMER_TEXT_3", "CUSTOMER_TEXT_4", "CUSTOMER_TEXT_5", "CUSTOMER_TEXT_6", "CUSTOMER_NUMBER_1", "CUSTOMER_NUMBER_2", "CUSTOMER_NUMBER_3", "CUSTOMER_DATE_1", "CUSTOMER_DATE_2", "CUSTOMER_DATE_3", "SOURCE_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS"
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Opportunity__dbt_tmp
    );
