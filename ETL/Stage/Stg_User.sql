{% set results = get_column_values_from_query("select * from " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".DIM_SALES_ENTITY where ENTITY_TYPE = 'SF'", 'ENTITY_NAME')%}

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
        unique_key= 'User_id'
      )
}}

{% for V_SF_Schema in results %}

 {% if  V_SF_Schema[0:2] == 'SF'  %} 
    
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
        '{{ V_SF_Schema }}' as Source_type,
        'D_USER_ROLE_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ V_SF_Schema }}.user
       {% if not loop.last %}
            UNION ALL
        {% endif %}    
    {% endif %}
{% endfor %}