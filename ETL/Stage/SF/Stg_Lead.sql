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
        unique_key= 'lead_id',
        post_hook="DELETE FROM " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".STG_LEAD WHERE lead_id IS NULL"
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
        {{ dbt_utils.surrogate_key('id') }}  AS lead_id,
        ID as Source_ID,
        IS_DELETED,
        MASTER_RECORD_ID,
        LAST_NAME,
        FIRST_NAME,
        SALUTATION,
        NAME,
        TITLE,
        COMPANY,
        STREET,
        CITY,
        STATE,
        POSTAL_CODE,
        COUNTRY,
        LATITUDE,
        LONGITUDE,
        GEOCODE_ACCURACY,
        PHONE,
        EMAIL,
        WEBSITE,
        PHOTO_URL,
        DESCRIPTION,
        LEAD_SOURCE,
        STATUS,
        INDUSTRY,
        RATING,
        ANNUAL_REVENUE,
        NUMBER_OF_EMPLOYEES,
        OWNER_ID,
        IS_CONVERTED,
        CONVERTED_DATE,
        CONVERTED_ACCOUNT_ID,
        CONVERTED_CONTACT_ID,
        CONVERTED_OPPORTUNITY_ID,
        IS_UNREAD_BY_OWNER,
        CREATED_DATE,
        CREATED_BY_ID,
        LAST_MODIFIED_DATE,
        LAST_MODIFIED_BY_ID,
        SYSTEM_MODSTAMP,
        LAST_ACTIVITY_DATE,
        LAST_VIEWED_DATE,
        LAST_REFERENCED_DATE,
        JIGSAW,
        JIGSAW_CONTACT_ID,
        EMAIL_BOUNCED_REASON,
        EMAIL_BOUNCED_DATE,
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
        'D_LEAD_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ schema_nm }}.Lead
        {% if not loop.last %}
            UNION ALL
        {% endif %}  
{% elif  entity_typ == 'X'  %}     
       select
        null as lead_id,
        null as  Source_ID,
        null as IS_DELETED,
        null as MASTER_RECORD_ID,
        null as LAST_NAME,
        null as FIRST_NAME,
        null as SALUTATION,
        null as NAME,
        null as TITLE,
        null as COMPANY,
        null as STREET,
        null as CITY,
        null as STATE,
        null as POSTAL_CODE,
        null as COUNTRY,
        null as LATITUDE,
        null as LONGITUDE,
        null as GEOCODE_ACCURACY,
        null as PHONE,
        null as EMAIL,
        null as WEBSITE,
        null as PHOTO_URL,
        null as DESCRIPTION,
        null as LEAD_SOURCE,
        null as STATUS,
        null as INDUSTRY,
        null as RATING,
        null as ANNUAL_REVENUE,
        null as NUMBER_OF_EMPLOYEES,
        null as OWNER_ID,
        null as IS_CONVERTED,
        null as CONVERTED_DATE,
        null as CONVERTED_ACCOUNT_ID,
        null as CONVERTED_CONTACT_ID,
        null as CONVERTED_OPPORTUNITY_ID,
        null as IS_UNREAD_BY_OWNER,
        null as CREATED_DATE,
        null as CREATED_BY_ID,
        null as LAST_MODIFIED_DATE,
        null as LAST_MODIFIED_BY_ID,
        null as SYSTEM_MODSTAMP,
        null as LAST_ACTIVITY_DATE,
        null as LAST_VIEWED_DATE,
        null as LAST_REFERENCED_DATE,
        null as JIGSAW,
        null as JIGSAW_CONTACT_ID,
        null as EMAIL_BOUNCED_REASON,
        null as EMAIL_BOUNCED_DATE,
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