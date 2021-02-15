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
        unique_key= 'Contact_ID'
      )
}}

{% for V_SF_Schema in results %}
 {% if  V_SF_Schema[0:2] == 'SF'  %}  
       
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
        '{{ V_SF_Schema }}' as Source_type,
        'D_CONTACT_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ V_SF_Schema }}.Contact
        {% if not loop.last %}
            UNION ALL
        {% endif %}  
    {% endif %}
{% endfor %}