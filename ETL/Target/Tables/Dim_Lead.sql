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
        unique_key= 'lead_id'
      )
}}

    WITH lead AS (
       select *  from {{ var('V_SF_Schema') }}.Lead
    ),opportunity as(
        select *  from {{ var('V_SF_Schema') }}.Opportunity 
    ),Dim_Lead as(
    SELECT
        {{ dbt_utils.surrogate_key('lead.id') }} AS lead_id,
        lead.LEAD_SOURCE AS LEAD_SOURCE,
        lead.ID AS source_id,
        concat(COALESCE(lead.street,' ',lead.city,' ',lead.state,' ',lead.postal_code,' ',lead.country)) AS lead_contact_address,
        lead.STATUS AS STATUS,
        opportunity.STAGE_NAME AS STAGE_NAME,
        NULL AS product_id,
        NULL AS campaign_id,
        lead.IS_CONVERTED AS lead_to_opp_flag,
        null AS lead_lost_flag,
        lead.CONVERTED_DATE AS lead_CONVERTED_DATE,
        lead.CONVERTED_OPPORTUNITY_ID AS CONVERTED_OPPORTUNITY_ID,
        NULL AS lead_lost_dt,
        NULL AS lead_lost_reason,
        lead.INDUSTRY AS Industry,
        LEAD.owner_id AS employee_id,
        lead.CREATED_DATE as INITIAL_CREATE_DT,
        lead.LAST_MODIFIED_DATE as lead_LAST_MODIFIED_DATE,
        {% if var("V_SF_CRM_ETL") == 'FIVETRAN_SF' %}  'SF' {% endif %} as Source_type,
        'D_LEAD_DIM_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
        FROM
          lead left join opportunity on lead.CONVERTED_OPPORTUNITY_ID = opportunity.id 
    )    
 
select * from Dim_Lead

