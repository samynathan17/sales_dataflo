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
        unique_key= 'Contact_ID',
        post_hook="DELETE FROM " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".STG_CONTACT WHERE Contact_ID IS NULL"
      )
}}

{% for V_SF_Schema in results %}

{% if V_SF_Schema != 'X' %} 
{% set schema_nm,entity_typ = V_SF_Schema.split('#') %}
{% else %}
{% set entity_typ = 'X' %}
{% endif %}

 {% if  entity_typ == 'SF'  %}  
       
  select distinct
        {{ dbt_utils.surrogate_key('id') }}  AS Contact_ID,
        ID as Source_ID,
        IS_DELETED,
        MASTER_RECORD_ID,
        ACCOUNT_ID,
        LAST_NAME,
        FIRST_NAME,
        SALUTATION,
        NAME,
        OTHER_STREET,
        OTHER_CITY,
        OTHER_STATE,
        OTHER_POSTAL_CODE,
        OTHER_COUNTRY,
        MAILING_STREET,
        MAILING_CITY,
        MAILING_STATE,
        MAILING_POSTAL_CODE,
        MAILING_COUNTRY,
        PHONE,
        FAX,
        MOBILE_PHONE,
        HOME_PHONE,
        OTHER_PHONE,
        ASSISTANT_PHONE,
        REPORTS_TO_ID,
        EMAIL,
        TITLE,
        DEPARTMENT,
        ASSISTANT_NAME,
        LEAD_SOURCE,
        BIRTHDATE,
        DESCRIPTION,
        OWNER_ID,
        CREATED_DATE,
        CREATED_BY_ID,
        LAST_MODIFIED_DATE,
        LAST_MODIFIED_BY_ID,
        EMAIL_BOUNCED_REASON,
        EMAIL_BOUNCED_DATE,
        IS_EMAIL_BOUNCED,
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
        'D_CONTACT_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ schema_nm }}.Contact
        {% if not loop.last %}
            UNION ALL
        {% endif %}  
   {% elif  V_SF_Schema[0:1] == 'X'  %}     
       select
        null as Contact_ID,
        null as  Source_ID,
        null as IS_DELETED,
        null as MASTER_RECORD_ID,
        null as ACCOUNT_ID,
        null as LAST_NAME,
        null as FIRST_NAME,
        null as SALUTATION,
        null as NAME,
        null as OTHER_STREET,
        null as OTHER_CITY,
        null as OTHER_STATE,
        null as OTHER_POSTAL_CODE,
        null as OTHER_COUNTRY,
        null as MAILING_STREET,
        null as MAILING_CITY,
        null as MAILING_STATE,
        null as MAILING_POSTAL_CODE,
        null as MAILING_COUNTRY,
        null as PHONE,
        null as FAX,
        null as MOBILE_PHONE,
        null as HOME_PHONE,
        null as OTHER_PHONE,
        null as ASSISTANT_PHONE,
        null as REPORTS_TO_ID,
        null as EMAIL,
        null as TITLE,
        null as DEPARTMENT,
        null as ASSISTANT_NAME,
        null as LEAD_SOURCE,
        null as BIRTHDATE,
        null as DESCRIPTION,
        null as OWNER_ID,
        null as CREATED_DATE,
        null as CREATED_BY_ID,
        null as LAST_MODIFIED_DATE,
        null as LAST_MODIFIED_BY_ID,
        null as EMAIL_BOUNCED_REASON,
        null as EMAIL_BOUNCED_DATE,
        null as IS_EMAIL_BOUNCED,
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