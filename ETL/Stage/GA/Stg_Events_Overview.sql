{% set results = get_column_values_from_query("select * from " ~ var('V_DB') ~ "." ~ var('V_Entity_Schema')~ "." ~ var('V_Mkt')~" where DATASOURCE_TYPE = 'GA' and READY_TO_PROCESS = 'TRUE'", "ENTITY_DATASORUCE_NAME||'#'||DATASOURCE_TYPE")%}

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
        unique_key= 'ID',
        post_hook="DELETE FROM " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".STG_EVENTS_OVERVIEW WHERE ID IS NULL"
      )
}}

{% for V_SF_Schema in results %}

{% if V_SF_Schema != 'X' %} 
{% set schema_nm,entity_typ = V_SF_Schema.split('#') %}
{% else %}
{% set entity_typ = 'X' %}
{% endif %}

 {% if  entity_typ == 'GA'  %}  
    
  select
        {{ dbt_utils.surrogate_key('_FIVETRAN_ID','PROFILE','DATE') }}  AS ID,
        DATE,
        PROFILE,
        EVENT_CATEGORY,
        EVENT_VALUE,
        TOTAL_EVENTS,
        SESSIONS_WITH_EVENT,
        EVENTS_PER_SESSION_WITH_EVENT,
        AVG_EVENT_VALUE,
        UNIQUE_EVENTS,
        '{{ schema_nm }}' as Source_type,
        'D_EVENTS_OVERVIEW_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ schema_nm }}.EVENTS_OVERVIEW
    {% if not loop.last %}
            UNION ALL
        {% endif %}
    {% elif  entity_typ == 'X'  %}     
       select
        null as DATE,
        null as PROFILE,
        null as EVENT_CATEGORY,
        null as EVENT_VALUE,
        null as TOTAL_EVENTS,
        null as SESSIONS_WITH_EVENT,
        null as EVENTS_PER_SESSION_WITH_EVENT,
        null as AVG_EVENT_VALUE,
        null as UNIQUE_EVENTS
        from dual

    
        
    {% endif %}
{% endfor %}