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
        post_hook="DELETE FROM " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".STG_GOAL_CONVERSIONS WHERE ID IS NULL"
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
        GOAL_COMPLETION_LOCATION,
        GOAL_PREVIOUS_STEP_1,
        GOAL_PREVIOUS_STEP_2,
        GOAL_PREVIOUS_STEP_3,
        GOAL_VALUE_ALL,
        GOAL_COMPLETIONS_ALL,
        GOAL_STARTS_ALL,
        GOAL_CONVERSION_RATE_ALL,
        GOAL_ABANDON_RATE_ALL,
        GOAL_ABANDONS_ALL,
        GOAL_VALUE_PER_SESSION,        
        '{{ schema_nm }}' as Source_type,
        'D_GOAL_CONVERSIONS_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ schema_nm }}.GOAL_CONVERSIONS
          {% if not loop.last %}
            UNION ALL
        {% endif %}
                {% elif  entity_typ == 'X'  %}  
                select
        null as DATE,
        null as PROFILE,
        null as GOAL_COMPLETION_LOCATION,
        null as GOAL_PREVIOUS_STEP_1,
        null as GOAL_PREVIOUS_STEP_2,
        null as GOAL_PREVIOUS_STEP_3,
        null as GOAL_VALUE_ALL,
        null as GOAL_COMPLETIONS_ALL,
        null as GOAL_STARTS_ALL,
        null as GOAL_CONVERSION_RATE_ALL,
        null as GOAL_ABANDON_RATE_ALL,
        null as GOAL_ABANDONS_ALL,
        null as GOAL_VALUE_PER_SESSION
        from dual
    {% endif %}
{% endfor %}