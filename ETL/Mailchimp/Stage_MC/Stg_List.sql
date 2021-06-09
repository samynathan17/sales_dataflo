{% set results = get_column_values_from_query("select * from " ~ var('V_DB') ~ "." ~ var('V_Entity_Schema')~ "." ~ var('V_Sales')~" where DATASOURCE_TYPE = 'MC' and READY_TO_PROCESS = 'TRUE'", "ENTITY_DATASORUCE_NAME||'#'||DATASOURCE_TYPE")%}

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
        unique_key= 'LIST_ID',
        post_hook="DELETE FROM " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".STG_LIST WHERE LIST_ID IS NULL"
      )
}}


{% for V_SF_Schema in results %}

{% if V_SF_Schema != 'X' %} 
{% set schema_nm,entity_typ = V_SF_Schema.split('#') %}
{% else %}
{% set entity_typ = 'X' %}
{% endif %}

 {% if  entity_typ == 'MC'  %}   
      
  select
        {{ dbt_utils.surrogate_key('ID') }}  AS LIST_ID,
        ID as Source_ID,
        NAME,
        CONTACT_COMPANY,
        CONTACT_ADDRESS_1,
        CONTACT_ADDRESS_2,
        CONTACT_CITY,
        CONTACT_STATE,
        CONTACT_ZIP,
        CONTACT_COUNTRY,
        VISIBILITY,
        _FIVETRAN_DELETED,
        PERMISSION_REMINDER,
        USE_ARCHIVE_BAR,
        DEFAULT_SUBJECT,
        DEFAULT_LANGUAGE,
        DEFAULT_FROM_NAME,
        DEFAULT_FROM_EMAIL,
        NOTIFY_ON_SUBSCRIBE,
        NOTIFY_ON_UNSUBSCRIBE,
        DATE_CREATED,
        LIST_RATING,
        EMAIL_TYPE_OPTION,
        SUBSCRIBE_URL_SHORT,
        SUBSCRIBE_URL_LONG,
        BEAMER_ADDRESS,
        _FIVETRAN_SYNCED,
        '{{ schema_nm }}' as Source_type,
        'D_CAMPAIGN_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ schema_nm }}.CAMPAIGN_RECIPIENT
         {% if not loop.last %}
            UNION ALL
        {% endif %}
    {% elif  V_SF_Schema[0:1] == 'X'  %}     
       select
        null as LIST_ID,
        null AS Source_ID,
        null AS NAME,
        null AS CONTACT_COMPANY,
        null AS CONTACT_ADDRESS_1,
        null AS CONTACT_ADDRESS_2,
        null AS CONTACT_CITY,
        null AS CONTACT_STATE,
        null AS CONTACT_ZIP,
        null AS CONTACT_COUNTRY,
        null AS VISIBILITY,
        null AS _FIVETRAN_DELETED,
        null AS PERMISSION_REMINDER,
        null AS USE_ARCHIVE_BAR,
        null AS DEFAULT_SUBJECT,
        null AS DEFAULT_LANGUAGE,
        null AS DEFAULT_FROM_NAME,
        null AS DEFAULT_FROM_EMAIL,
        null AS NOTIFY_ON_SUBSCRIBE,
        null AS NOTIFY_ON_UNSUBSCRIBE,
        null AS DATE_CREATED,
        null AS LIST_RATING,
        null AS EMAIL_TYPE_OPTION,
        null AS SUBSCRIBE_URL_SHORT,
        null AS SUBSCRIBE_URL_LONG,
        null AS BEAMER_ADDRESS,
        null AS _FIVETRAN_SYNCED,
        null as Source_type,
        null AS DW_SESSION_NM,   
    FROM dual    
    {% endif %}
{% endfor %}