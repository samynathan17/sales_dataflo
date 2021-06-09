{% if not var("enable_SF_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

{{
    config(
        materialized='incremental',
        unique_key= 'Opportunity_Calc_id'
      )
}}

WITH opportunity_history AS (
       select *  from {{ ref('Stg_Opportunity_History') }} 
),opportunity as(
        select *  from {{ ref('Stg_Opportunity') }} 
),Dim_Opportunity_Calc as(
    SELECT
        opportunity_history.Opportunity_Calc_id as Opportunity_Calc_id,
        opportunity_history.OPPORTUNITY_ID as opp_calc_stage_id,
        opportunity_history.CREATED_DATE as opp_calc_stage_start_datetime,
        case when opportunity_history.SYSTEM_MODSTAMP = opportunity_history.CREATED_DATE then NULL 
        else opportunity_history.SYSTEM_MODSTAMP end as opp_calc_stage_end_datetime,
        opportunity_history.STAGE_NAME as opp_calc_stage_name,
        opportunity_history.AMOUNT as opp_calc_AMOUNT,
        opportunity_history.EXPECTED_REVENUE  as opp_calc_EXPECTED_REVENUE,
        opportunity_history.CLOSE_DATE  as opp_calc_CLOSE_DATE,
        opportunity_history.PROBABILITY  as opp_calc_PROBABILITY,
        opportunity_history.FORECAST_CATEGORY  as opp_calc_FORECAST_CATEGORY,
        --opportunity_history.CURRENCY_ISO_CODE  as opp_calc_CURRENCY_ISO_CODE,
        opportunity_history.IS_DELETED as opp_calc_IS_DELETED,
        opportunity_history.PREV_AMOUNT  as opp_calc_PREV_AMOUNT,
        opportunity_history.PREV_CLOSE_DATE  as opp_calc_PREV_CLOSE_DATE,
        case when opportunity_history.SYSTEM_MODSTAMP = opportunity_history.CREATED_DATE then 'Y' else 'N' end  as Active_Flag,
        opportunity_history.Source_type AS Source_type,
        'D_OPPORTUNITY_CALC_DIM_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
        FROM
          opportunity join opportunity_history 
          on opportunity_history.OPPORTUNITY_ID = opportunity.Source_ID 
          and opportunity_history.Source_type = opportunity.Source_type  
)  
 
select * from Dim_Opportunity_Calc

