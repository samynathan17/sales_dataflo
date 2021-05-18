
    delete from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Opportunity_Stage
    where (Stage_id) in (
        select (Stage_id)
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Opportunity_Stage__dbt_tmp
    );
    

    insert into DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Opportunity_Stage ("STAGE_ID", "SOURCE_ID", "MASTER_LABEL", "API_NAME", "IS_ACTIVE", "SORT_ORDER", "IS_CLOSED", "IS_WON", "FORECAST_CATEGORY", "FORECAST_CATEGORY_NAME", "DEFAULT_PROBABILITY", "DESCRIPTION", "CREATED_BY_ID", "CREATED_DATE", "LAST_MODIFIED_BY_ID", "LAST_MODIFIED_DATE", "CUSTOMER_TEXT_1", "CUSTOMER_TEXT_2", "CUSTOMER_TEXT_3", "CUSTOMER_TEXT_4", "CUSTOMER_TEXT_5", "CUSTOMER_TEXT_6", "CUSTOMER_NUMBER_1", "CUSTOMER_NUMBER_2", "CUSTOMER_NUMBER_3", "CUSTOMER_DATE_1", "CUSTOMER_DATE_2", "CUSTOMER_DATE_3", "SOURCE_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS")
    (
        select "STAGE_ID", "SOURCE_ID", "MASTER_LABEL", "API_NAME", "IS_ACTIVE", "SORT_ORDER", "IS_CLOSED", "IS_WON", "FORECAST_CATEGORY", "FORECAST_CATEGORY_NAME", "DEFAULT_PROBABILITY", "DESCRIPTION", "CREATED_BY_ID", "CREATED_DATE", "LAST_MODIFIED_BY_ID", "LAST_MODIFIED_DATE", "CUSTOMER_TEXT_1", "CUSTOMER_TEXT_2", "CUSTOMER_TEXT_3", "CUSTOMER_TEXT_4", "CUSTOMER_TEXT_5", "CUSTOMER_TEXT_6", "CUSTOMER_NUMBER_1", "CUSTOMER_NUMBER_2", "CUSTOMER_NUMBER_3", "CUSTOMER_DATE_1", "CUSTOMER_DATE_2", "CUSTOMER_DATE_3", "SOURCE_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS"
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Opportunity_Stage__dbt_tmp
    );
