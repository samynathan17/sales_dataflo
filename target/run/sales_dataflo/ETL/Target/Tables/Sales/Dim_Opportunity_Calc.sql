
    delete from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Opportunity_Calc
    where (Opportunity_Calc_id) in (
        select (Opportunity_Calc_id)
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Opportunity_Calc__dbt_tmp
    );
    

    insert into DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Opportunity_Calc ("OPPORTUNITY_CALC_ID", "OPP_CALC_STAGE_ID", "OPP_CALC_STAGE_START_DATETIME", "OPP_CALC_STAGE_END_DATETIME", "OPP_CALC_STAGE_NAME", "OPP_CALC_AMOUNT", "OPP_CALC_EXPECTED_REVENUE", "OPP_CALC_CLOSE_DATE", "OPP_CALC_PROBABILITY", "OPP_CALC_FORECAST_CATEGORY", "OPP_CALC_IS_DELETED", "OPP_CALC_PREV_AMOUNT", "OPP_CALC_PREV_CLOSE_DATE", "ACTIVE_FLAG", "SOURCE_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS")
    (
        select "OPPORTUNITY_CALC_ID", "OPP_CALC_STAGE_ID", "OPP_CALC_STAGE_START_DATETIME", "OPP_CALC_STAGE_END_DATETIME", "OPP_CALC_STAGE_NAME", "OPP_CALC_AMOUNT", "OPP_CALC_EXPECTED_REVENUE", "OPP_CALC_CLOSE_DATE", "OPP_CALC_PROBABILITY", "OPP_CALC_FORECAST_CATEGORY", "OPP_CALC_IS_DELETED", "OPP_CALC_PREV_AMOUNT", "OPP_CALC_PREV_CLOSE_DATE", "ACTIVE_FLAG", "SOURCE_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS"
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Opportunity_Calc__dbt_tmp
    );
