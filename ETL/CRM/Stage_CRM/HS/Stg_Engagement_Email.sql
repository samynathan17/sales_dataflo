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
        unique_key= 'Engagement_Email_ID',
        post_hook="DELETE FROM " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".STG_ENGAGEMENT_EMAIL WHERE Engagement_Email_ID IS NULL"
      )
}}

{% for V_SF_Schema in results %}

{% if V_SF_Schema != 'X' %} 
{% set schema_nm,entity_typ = V_SF_Schema.split('#') %}
{% else %}
{% set entity_typ = 'X' %}
{% endif %}

 {% if  entity_typ == 'HS'  %} 
 
  select
        {{ dbt_utils.surrogate_key('ENGAGEMENT_ID') }}  AS Engagement_Email_ID,
        ENGAGEMENT_ID,
        FROM_EMAIL,
        FROM_FIRST_NAME,
        FROM_LAST_NAME,
        SUBJECT,
        HTML,
        TEXT,
        TRACKER_KEY,
        MESSAGE_ID,
        THREAD_ID,
        STATUS,
        SENT_VIA,
        LOGGED_FROM,
        ERROR_MESSAGE,
        FACSIMILE_SEND_ID,
        POST_SEND_STATUS,
        MEDIA_PROCESSING_STATUS,
        ATTACHED_VIDEO_OPENED,
        ATTACHED_VIDEO_WATCHED,
        ATTACHED_VIDEO_ID,
        RECIPIENT_DROP_REASONS,
        VALIDATION_SKIPPED,
        EMAIL_SEND_EVENT_ID_CREATED,
        EMAIL_SEND_EVENT_ID_ID,
        PENDING_INLINE_IMAGE_IDS,
        _FIVETRAN_SYNCED,
        '{{ schema_nm }}' as Source_type,
        'D_COMPANY_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS    
    FROM {{ schema_nm }}.ENGAGEMENT_EMAIL
     {% if not loop.last %}
            UNION ALL
        {% endif %}
    {% elif  entity_typ == 'X'  %}     
       select
        null  AS Engagement_Email_ID,
        null  AS Engagement_ID,
        null  AS Source_ID,
        null  AS PORTAL_ID,
        null  AS ACTIVE,
        null  AS OWNER_ID,
        null  AS TYPE,
        null  AS ACTIVITY_TYPE,
        null  AS CREATED_AT,
        null  AS LAST_UPDATED,
        null  AS TIMESTAMP,
        null  AS _FIVETRAN_SYNCED,
        '{{ schema_nm }}' as Source_type,
        'D_COMPANY_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM dual      
    {% endif %}
{% endfor %}

