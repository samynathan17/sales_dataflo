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
        post_hook="DELETE FROM " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".STG_ADWORDS_KEYWORD WHERE ID IS NULL"
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
        KEYWORD,
        GOAL_VALUE_ALL,
        SESSIONS,
        GOAL_COMPLETIONS_ALL,
        PAGEVIEWS_PER_SESSION,
        GOAL_CONVERSION_RATE_ALL,
        USERS,
        BOUNCE_RATE,
        AD_CLICKS,
        AD_COST,
        CPC,
        '{{ schema_nm }}' as Source_type,
        'D_ADWORDS_KEYWORD_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ schema_nm }}.ADWORDS_KEYWORD
          {% if not loop.last %}
            UNION ALL
        {% endif %}
        {% elif  entity_typ == 'X'  %} 
         select
        null as DATE,
        null as PROFILE,
        null as KEYWORD,
        null as GOAL_VALUE_ALL,
        null as SESSIONS,
        null as GOAL_COMPLETIONS_ALL,
        null as PAGEVIEWS_PER_SESSION,
        null as GOAL_CONVERSION_RATE_ALL,
        null as USERS,
        null as BOUNCE_RATE,
        null as AD_CLICKS,
        null as AD_COST,
        null as CPC
        from dual

    {% endif %}
{% endfor %}