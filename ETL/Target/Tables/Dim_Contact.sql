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
        unique_key= 'contact_id'
      )
}}

WITH contacts AS (
       select * from {{ ref('Stg_Contact') }} 
    ),
Dim_Contact as(
      SELECT 
        contact_id, 
        contacts.salutation AS salutation, 
        contacts.NAME AS contact_name, 
        contacts.last_name AS last_name, 
        contacts.first_name AS first_name, 
        contacts.phone AS contact_number, 
        contacts.mobile_phone AS mobile_phone, 
        contacts.home_phone AS home_phone, 
        contacts.email AS contact_email, 
        source_id, 
        contacts.department AS department, 
        contacts.lead_source AS lead_source, 
        null AS organization_id, 
        contacts.owner_ID AS employee_id, 
        contacts.account_id AS account_id, 
        NULL AS contact_age_group, 
        NULL AS contact_income, 
        NULL AS dependent, 
        NULL AS contact_type, 
        contacts.IS_DELETED AS active,
        CREATED_DATE as INITIAL_CREATE_DT,
        {% if var("V_SF_CRM_ETL") == 'FIVETRAN_SF' %}  'SF' {% endif %} as Source_type,
        'D_CONTACT_DIM_LOAD' AS dw_session_nm, 
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
     FROM 
       contacts
    )    
    
select * from Dim_Contact

