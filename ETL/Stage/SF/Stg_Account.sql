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
        unique_key= 'Account_ID',
        post_hook="DELETE FROM " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".STG_ACCOUNT WHERE ACCOUNT_ID IS NULL"
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
        {{ dbt_utils.surrogate_key('id') }}  AS Account_ID,
        ID as Source_ID,
        IS_DELETED,
        MASTER_RECORD_ID,
        NAME,
        TYPE,
        PARENT_ID,
        BILLING_STREET,
        BILLING_CITY,
        BILLING_STATE,
        BILLING_POSTAL_CODE,
        BILLING_COUNTRY,
        SHIPPING_STREET,
        SHIPPING_CITY,
        SHIPPING_STATE,
        SHIPPING_POSTAL_CODE,
        SHIPPING_COUNTRY,
        PHONE,
        FAX,
        WEBSITE,
        SIC,
        INDUSTRY,
        ANNUAL_REVENUE,
        NUMBER_OF_EMPLOYEES,
        OWNERSHIP,
        DESCRIPTION,
        OWNER_ID,
        CREATED_DATE,
        CREATED_BY_ID,
        LAST_MODIFIED_DATE,
        LAST_MODIFIED_BY_ID,
        ACCOUNT_SOURCE,
        SIC_DESC,
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
        'D_ACCOUNT_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ schema_nm }}.Account
         {% if not loop.last %}
            UNION ALL
        {% endif %}
    {% elif  V_SF_Schema[0:1] == 'X'  %}     
       select
        null as Account_ID,
        null as  Source_ID,
        null as IS_DELETED,
        null as MASTER_RECORD_ID,
        null as NAME,
        null as TYPE,
        null as PARENT_ID,
        null as BILLING_STREET,
        null as BILLING_CITY,
        null as BILLING_STATE,
        null as BILLING_POSTAL_CODE,
        null as BILLING_COUNTRY,
        null as SHIPPING_STREET,
        null as SHIPPING_CITY,
        null as SHIPPING_STATE,
        null as SHIPPING_POSTAL_CODE,
        null as SHIPPING_COUNTRY,
        null as PHONE,
        null as FAX,
        null as WEBSITE,
        null as SIC,
        null as INDUSTRY,
        null as ANNUAL_REVENUE,
        null as NUMBER_OF_EMPLOYEES,
        null as OWNERSHIP,
        null as DESCRIPTION,
        null as OWNER_ID,
        null as CREATED_DATE,
        null as CREATED_BY_ID,
        null as LAST_MODIFIED_DATE,
        null as LAST_MODIFIED_BY_ID,
        null as ACCOUNT_SOURCE,
        null as SIC_DESC,
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