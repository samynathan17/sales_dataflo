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
        post_hook="DELETE FROM " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".STG_CHANNEL_TRAFFIC WHERE ID IS NULL"
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
        CHANNEL_GROUPING,
        GOAL_VALUE_ALL,
        NEW_USERS,
        SESSIONS,
        AVG_SESSION_DURATION,
        GOAL_COMPLETIONS_ALL,
        PAGEVIEWS_PER_SESSION,
        GOAL_CONVERSION_RATE_ALL,
        USERS,
        BOUNCE_RATE,
        PERCENT_NEW_SESSIONS,   
        '{{ schema_nm }}' as Source_type,
        'D_CHANNEL_TRAFFIC_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ schema_nm }}.CHANNEL_TRAFFIC 
          {% if not loop.last %}
            UNION ALL
        {% endif %} 
        {% elif  entity_typ == 'X'  %} 
        select
        null as DATE,
        null as PROFILE,
        null as CHANNEL_GROUPING,
        null as GOAL_VALUE_ALL,
        null as NEW_USERS,
        null as SESSIONS,
        null as AVG_SESSION_DURATION,
        null as GOAL_COMPLETIONS_ALL,
        null as PAGEVIEWS_PER_SESSION,
        null as GOAL_CONVERSION_RATE_ALL,
        null as USERS,
        null as BOUNCE_RATE,
        null as PERCENT_NEW_SESSIONS
        from dual
   

    {% endif %}
{% endfor %}