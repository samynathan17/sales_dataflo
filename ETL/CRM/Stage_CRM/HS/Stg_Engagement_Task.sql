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
        unique_key= 'Engagement_Task_ID',
        post_hook="DELETE FROM " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".STG_ENGAGEMENT_TASK WHERE ENGAGEMENT_TASK_ID IS NULL"
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
        {{ dbt_utils.surrogate_key('ENGAGEMENT_ID') }}  AS ENGAGEMENT_TASK_ID,
        ENGAGEMENT_ID,
        BODY,
        SUBJECT,
        STATUS,
        FOR_OBJECT_TYPE,
        TASK_TYPE,
        _FIVETRAN_SYNCED,
        SEQUENCE_STEP_ORDER,
        COMPLETION_DATE,
        '{{ schema_nm }}' as Source_type,
        'D_COMPANY_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS    
    FROM {{ schema_nm }}.ENGAGEMENT_TASK
     {% if not loop.last %}
            UNION ALL
        {% endif %}
    {% elif  entity_typ == 'X'  %}     
       select
        null  AS ENGAGEMENT_TASK_ID,
        null  AS ENGAGEMENT_ID
        null  AS BODY,
        null  AS SUBJECT,
        null  AS STATUS,
        null  AS FOR_OBJECT_TYPE,
        null  AS TASK_TYPE,
        null  AS _FIVETRAN_SYNCED,
        null  AS SEQUENCE_STEP_ORDER,
        null  AS COMPLETION_DATE,      
        '{{ schema_nm }}' as Source_type,
        'D_COMPANY_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM dual      
    {% endif %}
{% endfor %}

