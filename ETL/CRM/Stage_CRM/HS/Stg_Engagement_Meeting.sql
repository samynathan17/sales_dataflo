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
        unique_key= 'ENGAGEMENT_Meeting_ID',
        post_hook="DELETE FROM " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".STG_ENGAGEMENT_MEETING WHERE ENGAGEMENT_Meeting_ID IS NULL"
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
        {{ dbt_utils.surrogate_key('ENGAGEMENT_ID') }}  AS ENGAGEMENT_Meeting_ID,
        ENGAGEMENT_ID,
        BODY,
        START_TIME,
        END_TIME,
        TITLE,
        EXTERNAL_URL,
        SOURCE,
        CREATED_FROM_LINK_ID,
        SOURCE_ID,
        WEB_CONFERENCE_MEETING_ID,
        MEETING_OUTCOME,
        PRE_MEETING_PROSPECT_REMINDERS,
        _FIVETRAN_SYNCED,
        '{{ schema_nm }}' as Source_type,
        'D_COMPANY_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS    
    FROM {{ schema_nm }}.ENGAGEMENT_MEETING
     {% if not loop.last %}
            UNION ALL
        {% endif %}
    {% elif  entity_typ == 'X'  %}     
       select
        null  AS ENGAGEMENT_Meeting_ID,
        null as ENGAGEMENT_ID,
        null  AS BODY,
        null  AS START_TIME,
        null  AS END_TIME,
        null  AS TITLE,
        null  AS EXTERNAL_URL,
        null  AS SOURCE,
        null  AS CREATED_FROM_LINK_ID,
        null  AS SOURCE_ID,
        null  AS WEB_CONFERENCE_MEETING_ID,
        null  AS MEETING_OUTCOME,
        null  AS PRE_MEETING_PROSPECT_REMINDERS,
        null  AS _FIVETRAN_SYNCED,
        '{{ schema_nm }}' as Source_type,
        'D_COMPANY_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM dual      
    {% endif %}
{% endfor %}

