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
        unique_key= 'User_id',
        post_hook="DELETE FROM " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".STG_USER WHERE User_id IS NULL"
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
        {{ dbt_utils.surrogate_key('id') }}  AS User_id,
        ID as Source_ID,
        USERNAME,
        LAST_NAME,
        FIRST_NAME,
        NAME,
        COMPANY_NAME,
        DIVISION,
        DEPARTMENT,
        TITLE,
        STREET,
        CITY,
        STATE,
        POSTAL_CODE,
        COUNTRY,
        EMAIL,
        EMAIL_PREFERENCES_AUTO_BCC,
        EMAIL_PREFERENCES_AUTO_BCC_STAY_IN_TOUCH,
        EMAIL_PREFERENCES_STAY_IN_TOUCH_REMINDER,
        SENDER_EMAIL,
        SENDER_NAME,
        SIGNATURE,
        STAY_IN_TOUCH_SUBJECT,
        STAY_IN_TOUCH_SIGNATURE,
        STAY_IN_TOUCH_NOTE,
        PHONE,
        FAX,
        MOBILE_PHONE,
        ALIAS,
        COMMUNITY_NICKNAME,
        BADGE_TEXT,
        IS_ACTIVE,
        TIME_ZONE_SID_KEY,
        USER_ROLE_ID,
        LOCALE_SID_KEY,
        RECEIVES_INFO_EMAILS,
        RECEIVES_ADMIN_INFO_EMAILS,
        EMAIL_ENCODING_KEY,
        USER_TYPE,
        LANGUAGE_LOCALE_KEY,
        EMPLOYEE_NUMBER,
        DELEGATED_APPROVER_ID,
        MANAGER_ID,
        LAST_LOGIN_DATE,
        CREATED_DATE,
        CREATED_BY_ID,
        LAST_MODIFIED_DATE,
        LAST_MODIFIED_BY_ID,
        contact_id,
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
    FROM {{ schema_nm }}.user
       {% if not loop.last %}
            UNION ALL
        {% endif %}    
{% elif  entity_typ == 'X'  %}     
       select
        null as User_id,
        null as  Source_ID,
        null as USERNAME,
        null as LAST_NAME,
        null as FIRST_NAME,
        null as NAME,
        null as COMPANY_NAME,
        null as DIVISION,
        null as DEPARTMENT,
        null as TITLE,
        null as STREET,
        null as CITY,
        null as STATE,
        null as POSTAL_CODE,
        null as COUNTRY,
        null as EMAIL,
        null as EMAIL_PREFERENCES_AUTO_BCC,
        null as EMAIL_PREFERENCES_AUTO_BCC_STAY_IN_TOUCH,
        null as EMAIL_PREFERENCES_STAY_IN_TOUCH_REMINDER,
        null as SENDER_EMAIL,
        null as SENDER_NAME,
        null as SIGNATURE,
        null as STAY_IN_TOUCH_SUBJECT,
        null as STAY_IN_TOUCH_SIGNATURE,
        null as STAY_IN_TOUCH_NOTE,
        null as PHONE,
        null as FAX,
        null as MOBILE_PHONE,
        null as ALIAS,
        null as COMMUNITY_NICKNAME,
        null as BADGE_TEXT,
        null as IS_ACTIVE,
        null as TIME_ZONE_SID_KEY,
        null as USER_ROLE_ID,
        null as LOCALE_SID_KEY,
        null as RECEIVES_INFO_EMAILS,
        null as RECEIVES_ADMIN_INFO_EMAILS,
        null as EMAIL_ENCODING_KEY,
        null as USER_TYPE,
        null as LANGUAGE_LOCALE_KEY,
        null as EMPLOYEE_NUMBER,
        null as DELEGATED_APPROVER_ID,
        null as MANAGER_ID,
        null as LAST_LOGIN_DATE,
        null as CREATED_DATE,
        null as CREATED_BY_ID,
        null as LAST_MODIFIED_DATE,
        null as LAST_MODIFIED_BY_ID,
        null as contact_id,
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