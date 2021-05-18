



WITH source AS (
       select *  from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Opportunity_Stage  
    ),Dim_Opportunity_stage as(
        SELECT
        stage_id,
        MASTER_LABEL AS stage_name,
        SORT_ORDER AS stage_position,
        NULL AS account_id,
        IS_ACTIVE AS active_flag,
        Source_id,
        FORECAST_CATEGORY AS FORECAST_CATEGORY,
        NULL AS lead_opp_flag,
        IS_CLOSED AS IS_CLOSED,
        IS_WON AS IS_WON,
        DEFAULT_PROBABILITY AS OPPORTUNITY_STAGE,
        Source_type AS Source_type ,
        'D_OPPORTUNITYSTAGES_DIM_LOAD'  AS 	DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
     FROM
       source
    )    

select * from Dim_Opportunity_stage