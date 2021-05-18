{% set results = get_column_values_from_query("select * from " ~ var('V_DB') ~ "." ~ var('V_Entity_Schema')~ "." ~ var('V_Sales')~" where DATASOURCE_TYPE = 'HS'  and READY_TO_PROCESS = 'TRUE'", "ENTITY_DATASORUCE_NAME||'#'||DATASOURCE_TYPE")%}

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
        unique_key= 'OWNER_ID',
        post_hook="DELETE FROM " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".STG_OWNER WHERE OWNER_ID IS NULL"
      )
}}

{% for V_SF_Schema in results %}

{% if V_SF_Schema != 'X' %} 
{% set schema_nm,entity_typ = V_SF_Schema.split('#') %}
{% else %}
{% set entity_typ = 'X' %}
{% endif %}

 {% if  V_SF_Schema[0:2] == 'HS'  %}    
  select
        {{ dbt_utils.surrogate_key('OWNER_ID') }}  AS OWNER_ID,
        OWNER_ID as Source_OWNER_ID,
        PORTAL_ID,
        TYPE,
        FIRST_NAME,
        LAST_NAME,
        EMAIL,
        CREATED_AT,
        UPDATED_AT,
        IS_ACTIVE,
        ACTIVE_USER_ID,
        USER_ID_INCLUDING_INACTIVE,
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
        'D_OWNER_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS    
    FROM {{ schema_nm }}.Owner
        {% if not loop.last %}
            UNION ALL
        {% endif %}
    {% elif  entity_typ == 'X'  %}     
       select
        null as  OWNER_ID,
        null as  Source_OWNER_ID,
        null as PORTAL_ID,
        null as TYPE,
        null as FIRST_NAME,
        null as LAST_NAME,
        null as EMAIL,
        null as CREATED_AT,
        null as UPDATED_AT,
        null as IS_ACTIVE,
        null as ACTIVE_USER_ID,
        null as USER_ID_INCLUDING_INACTIVE, 
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
        NULL AS CUSTOMER_DATE_3,   
    FROM dual
    {% endif %}
{% endfor %}

