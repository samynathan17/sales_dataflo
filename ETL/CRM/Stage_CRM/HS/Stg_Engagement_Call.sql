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
        unique_key= 'Engagement_ID',
        post_hook="DELETE FROM " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".STG_ENGAGEMENT WHERE Engagement_ID IS NULL"
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
        {{ dbt_utils.surrogate_key('ENGAGEMENT_ID') }}  AS Engagement_Call_ID,
        ENGAGEMENT_ID,
        TO_NUMBER,
        FROM_NUMBER,
        STATUS,
        EXTERNAL_ID,
        DURATION_MILLISECONDS,
        EXTERNAL_ACCOUNT_ID,
        RECORDING_URL,
        BODY,
        DISPOSITION,
        CALLEE_OBJECT_TYPE,
        CALLEE_OBJECT_ID,
        TRANSCRIPTION_ID,
        UNKNOWN_VISITOR_CONVERSATION,
        SOURCE,
        TITLE,
        _FIVETRAN_SYNCED,
        '{{ schema_nm }}' as Source_type,
        'D_COMPANY_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS    
    FROM {{ schema_nm }}.ENGAGEMENT_CALL
     {% if not loop.last %}
            UNION ALL
        {% endif %}
    {% elif  entity_typ == 'X'  %}     
       select
        null  AS Engagement_Call_ID,
        null  AS ENGAGEMENT_ID,
        null  AS TO_NUMBER,
        null  AS FROM_NUMBER,
        null  AS STATUS,
        null  AS EXTERNAL_ID,
        null  AS DURATION_MILLISECONDS,
        null  AS EXTERNAL_ACCOUNT_ID,
        null  AS RECORDING_URL,
        null  AS BODY,
        null  AS DISPOSITION,
        null  AS CALLEE_OBJECT_TYPE,
        null  AS CALLEE_OBJECT_ID,
        null  AS TRANSCRIPTION_ID,
        null  AS UNKNOWN_VISITOR_CONVERSATION,
        null  AS SOURCE,
        null  AS TITLE,
        null  AS _FIVETRAN_SYNCED,
        '{{ schema_nm }}' as Source_type,
        'D_COMPANY_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM dual      
    {% endif %}
{% endfor %}

