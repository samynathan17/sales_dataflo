{% set results = get_column_values_from_query("select * from " ~ var('V_DB') ~ "." ~ var('V_Entity_Schema')~ "." ~ var('V_Sales')~" where DATASOURCE_TYPE = 'SF' and READY_TO_PROCESS = 'TRUE'", "ENTITY_DATASORUCE_NAME||'#'||DATASOURCE_TYPE")%}

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
        unique_key= 'User_Role_id',
        post_hook="DELETE FROM " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".STG_USER_ROLE WHERE User_Role_id IS NULL"
      )
}}

{% for V_SF_Schema in results %}

{% if V_SF_Schema != 'X' %} 
{% set schema_nm,entity_typ = V_SF_Schema.split('#') %}
{% else %}
{% set entity_typ = 'X' %}
{% endif %}

 {% if  entity_typ == 'SF'  %}  
select
        {{ dbt_utils.surrogate_key('id') }}  AS User_Role_id,
        ID as Source_ID,
        NAME,
        PARENT_ROLE_ID,
        ROLLUP_DESCRIPTION,
        OPPORTUNITY_ACCESS_FOR_ACCOUNT_OWNER,
        CASE_ACCESS_FOR_ACCOUNT_OWNER,
        CONTACT_ACCESS_FOR_ACCOUNT_OWNER,
        FORECAST_USER_ID,
        MAY_FORECAST_MANAGER_SHARE,
        LAST_MODIFIED_DATE,
        LAST_MODIFIED_BY_ID,
        SYSTEM_MODSTAMP,
        DEVELOPER_NAME,
        PORTAL_ACCOUNT_ID,
        PORTAL_TYPE,
        PORTAL_ACCOUNT_OWNER_ID,
         null AS CUSTOMER_TEXT_1,
        NULL AS CUSTOMER_TEXT_2,
        NULL AS CUSTOMER_TEXT_3,
        NULL AS CUSTOMER_TEXT_4,
        NULL AS CUSTOMER_TEXT_5,
        NULL AS CUSTOMER_TEXT_6,
        NULL AS CUSTOMER_NUMBER_1,
        NULL AS CUSTOMER_NUMBER_2,
        NULL AS CUSTOMER_NUMBER_3,
        NULL AS CUSTOMER_DATE_1,
        NULL AS CUSTOMER_DATE_2,
        NULL AS CUSTOMER_DATE_3,
        '{{ schema_nm }}' as Source_type,
        'D_USER_ROLE_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ schema_nm }}.user_role
            {% if not loop.last %}
               UNION ALL
            {% endif %}  
{% elif  entity_typ == 'X'  %}     
       select
        null as User_Role_id,
        null as  Source_ID,
        null as NAME,
        null as PARENT_ROLE_ID,
        null as ROLLUP_DESCRIPTION,
        null as OPPORTUNITY_ACCESS_FOR_ACCOUNT_OWNER,
        null as CASE_ACCESS_FOR_ACCOUNT_OWNER,
        null as CONTACT_ACCESS_FOR_ACCOUNT_OWNER,
        null as FORECAST_USER_ID,
        null as MAY_FORECAST_MANAGER_SHARE,
        null as LAST_MODIFIED_DATE,
        null as LAST_MODIFIED_BY_ID,
        null as SYSTEM_MODSTAMP,
        null as DEVELOPER_NAME,
        null as PORTAL_ACCOUNT_ID,
        null as PORTAL_TYPE,
        null as PORTAL_ACCOUNT_OWNER_ID,
        null as Source_type,
        null as DW_SESSION_NM,
        null as DW_INS_UPD_DTS,
         null AS CUSTOMER_TEXT_1,
        NULL AS CUSTOMER_TEXT_2,
        NULL AS CUSTOMER_TEXT_3,
        NULL AS CUSTOMER_TEXT_4,
        NULL AS CUSTOMER_TEXT_5,
        NULL AS CUSTOMER_TEXT_6,
        NULL AS CUSTOMER_NUMBER_1,
        NULL AS CUSTOMER_NUMBER_2,
        NULL AS CUSTOMER_NUMBER_3,
        NULL AS CUSTOMER_DATE_1,
        NULL AS CUSTOMER_DATE_2,
        NULL AS CUSTOMER_DATE_3   
    FROM dual    
    {% endif %}
{% endfor %}