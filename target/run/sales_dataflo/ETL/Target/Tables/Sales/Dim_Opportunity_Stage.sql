
    delete from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Opportunity_Stage
    where (Stage_id) in (
        select (Stage_id)
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Opportunity_Stage__dbt_tmp
    );
    

    insert into DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Opportunity_Stage ("STAGE_ID", "STAGE_NAME", "STAGE_POSITION", "ACCOUNT_ID", "ACTIVE_FLAG", "SOURCE_ID", "FORECAST_CATEGORY", "LEAD_OPP_FLAG", "IS_CLOSED", "IS_WON", "OPPORTUNITY_STAGE", "SOURCE_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS")
    (
        select "STAGE_ID", "STAGE_NAME", "STAGE_POSITION", "ACCOUNT_ID", "ACTIVE_FLAG", "SOURCE_ID", "FORECAST_CATEGORY", "LEAD_OPP_FLAG", "IS_CLOSED", "IS_WON", "OPPORTUNITY_STAGE", "SOURCE_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS"
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Opportunity_Stage__dbt_tmp
    );
