--depends_on: {{ ref('Stg_Opportunity') }}
--depends_on: {{ ref('Stg_Contact') }}
--depends_on: {{ ref('Stg_User') }}
--depends_on: {{ ref('Stg_Opportunity_Stage') }}
--depends_on: {{ ref('Stg_Deal') }}
--depends_on: {{ ref('Stg_Owner') }}
--depends_on: {{ ref('Stg_Deal_Pipeline_Stage') }}

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
        unique_key= 'opportunity_id'
      )
}}

(
WITH OPPORTUNITY AS (
       select *  from {{ ref('Stg_Opportunity') }}  
    ),contact AS(
        select *  from {{ ref('Stg_Contact') }}    
    ),
    emp AS(
        select *  from {{ ref('Stg_User') }}    
    ),
    OPPORTUNITY_STAGE AS(
        select *  from {{ ref('Stg_Opportunity_Stage') }}   
    )
    ,Dim_Opportunity as(
    SELECT
        opportunity_id,
        OPPORTUNITY.NAME AS opportunity_NAME,
        OPPORTUNITY.TYPE AS opportunity_Type, 
        cast ( OPPORTUNITY.ACCOUNT_ID as varchar(50)) AS ACCOUNT_ID,        
        cast ( OPPORTUNITY.OWNER_ID  as varchar(50)) AS employee_id,
        cast ( OPPORTUNITY.Source_id as varchar(50)) as Source_id,
        OPPORTUNITY_STAGE.SORT_ORDER AS stage_id,
        OPPORTUNITY.IS_WON as IS_WON,
        OPPORTUNITY.IS_CLOSED as IS_CLOSED,
        OPPORTUNITY.stage_name AS stage_name,
        --   OPPORTUNITY.PROBABILITY as PROBABILITY,        
        OPPORTUNITY.FORECAST_CATEGORY as FORECAST_CATEGORY,
        OPPORTUNITY.AMOUNT AS AMOUNT,
        NULL AS amount_without_disc,
        NULL AS expectd_Clouser_Dt,
        OPPORTUNITY.CONTACT_ID AS Contact_id,
        contact.NAME AS contact_name,
        contact.PHONE AS contact_number,
        contact.EMAIL AS contact_email,
        contact.MAILING_STREET AS contact_address,
        OPPORTUNITY.CREATED_DATE AS initial_create_dt,
        OPPORTUNITY.LAST_MODIFIED_DATE AS last_updated_dt,
        OPPORTUNITY.CLOSE_DATE AS CLOSE_DATE,
        NULL AS prospect_Dt,
        NULL AS stage_calc_id,
        OPPORTUNITY_STAGE.CREATED_DATE AS stage_start_dt,
        NULL AS stage_end_dt,        
        NULL AS lead_lost_reason,
        NULL AS competitor,
        NULL AS on_hold_flag,
        NULL AS sub_product_id,
        NULL AS sub_product_name,
        NULL AS prd_amount_without_disc,
        NULL AS prd_discount,
        OPPORTUNITY. IS_DELETED AS active_flag,
        NULL AS DW_CURR_FLG,
        NULL AS EFFCT_START_DATE,
        NULL AS EFFCT_END_DATE,
        OPPORTUNITY.Source_type as Source_type,
        'D_OPPORTUNITY_DIM_LOAD'  AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS
      FROM
        OPPORTUNITY 
        left join emp on OPPORTUNITY.OWNER_ID  =  emp.Source_ID
        left join contact on emp.contact_id  =  contact.Source_ID
        left join OPPORTUNITY_STAGE on OPPORTUNITY.stage_name = OPPORTUNITY_STAGE.MASTER_LABEL 
        and OPPORTUNITY_STAGE.Source_type = OPPORTUNITY.Source_type
    ) 
select * from Dim_Opportunity )   
Union all 
(WITH OPPORTUNITY AS (
       select *  from {{ ref('Stg_Deal') }}  
    ),
    emp AS(
        select *  from {{ ref('Stg_Owner') }}    
    ),
    OPPORTUNITY_STAGE AS(
        select *  from {{ ref('Stg_Deal_Pipeline_Stage') }}   
    )
    ,Dim_Opportunity as(
    SELECT
        DEAL_ID as opportunity_id,
        OPPORTUNITY.PROPERTY_DEALNAME AS opportunity_NAME,
        OPPORTUNITY.PROPERTY_DEALTYPE AS opportunity_Type, 
        NULL AS ACCOUNT_ID,        
        cast (OPPORTUNITY.OWNER_ID as varchar(50)) AS employee_id,
        cast (OPPORTUNITY.Source_DEAL_ID as varchar(50)) as Source_id,
        OPPORTUNITY_STAGE.DISPLAY_ORDER AS stage_id,
        null as IS_WON,
        OPPORTUNITY.PROPERTY_HS_IS_CLOSED as IS_CLOSED,
        OPPORTUNITY.DEAL_PIPELINE_STAGE_ID AS stage_name,
        --   OPPORTUNITY.PROBABILITY as PROBABILITY,        
        OPPORTUNITY.PROPERTY_HS_MANUAL_FORECAST_CATEGORY as FORECAST_CATEGORY,
        OPPORTUNITY.PROPERTY_AMOUNT AS AMOUNT,
        NULL AS amount_without_disc,
        NULL AS expectd_Clouser_Dt,
        NULL AS Contact_id,
        OPPORTUNITY.PROPERTY_POINT_OF_CONTACT AS contact_name,
        NULL AS contact_number,
        NULL AS contact_email,
        NULL AS contact_address,
        OPPORTUNITY.PROPERTY_CREATEDATE AS initial_create_dt,
        NULL AS last_updated_dt,
        OPPORTUNITY.PROPERTY_CLOSEDATE AS CLOSE_DATE,
        NULL AS prospect_Dt,
        NULL AS stage_calc_id,
        OPPORTUNITY.PROPERTY_HS_CREATEDATE AS stage_start_dt,
        NULL AS stage_end_dt,        
        NULL AS lead_lost_reason,
        NULL AS competitor,
        NULL AS on_hold_flag,
        NULL AS sub_product_id,
        NULL AS sub_product_name,
        NULL AS prd_amount_without_disc,
        NULL AS prd_discount,
        NULL AS active_flag,
        NULL AS DW_CURR_FLG,
        NULL AS EFFCT_START_DATE,
        NULL AS EFFCT_END_DATE,
        OPPORTUNITY.Source_type as Source_type,
        'D_OPPORTUNITY_DIM_LOAD'  AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS  
      FROM
        OPPORTUNITY 
        left join emp on OPPORTUNITY.Owner_id  =  emp.Source_OWNER_ID
        left join OPPORTUNITY_STAGE on OPPORTUNITY.DEAL_PIPELINE_STAGE_ID = OPPORTUNITY_STAGE.SOURCE_STAGE_ID 
        and OPPORTUNITY_STAGE.Source_type = OPPORTUNITY.Source_type
    )  

select * from Dim_Opportunity )
