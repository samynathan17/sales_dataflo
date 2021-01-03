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
        unique_key= 'Stage_id'
      )
}}

WITH source AS (
       select *  from {{ var('V_SF_Schema') }}.Opportunity_stage 
    ),Dim_Opportunity_stage as(
        SELECT
        {{ dbt_utils.surrogate_key('id') }}  AS stage_id,
        MASTER_LABEL AS stage_name,
        SORT_ORDER AS stage_position,
        NULL AS account_id,
        IS_ACTIVE AS active_flag,
        ID AS Source_id,
        FORECAST_CATEGORY AS FORECAST_CATEGORY,
        NULL AS lead_opp_flag,
        IS_CLOSED AS IS_CLOSED,
        IS_WON AS IS_WON,
        DEFAULT_PROBABILITY AS OPPORTUNITY_STAGE,
        {% if var("V_SF_CRM_ETL") == 'FIVETRAN_SF' %}  'SF' {% endif %} as Source_type,
        'D_OPPORTUNITYSTAGES_DIM_LOAD'  AS 	DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
     FROM
       source
    )    

select * from Dim_Opportunity_stage

