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
        unique_key= 'lead_id'
      )
}}

{% for V_SF_Schema in results %}
{% if  V_SF_Schema[0:2] == 'SF'  %}

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
        '{{ V_SF_Schema }}' as Source_type,
        'D_LEAD_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ V_SF_Schema }}.Lead
        {% if not loop.last %}
            UNION ALL
        {% endif %}  
     {% endif %}
{% endfor %}