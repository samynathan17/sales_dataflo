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
        post_hook="DELETE FROM " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".STG_SESSION WHERE ID IS NULL"
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
        SESSION_DURATION_BUCKET,
        USER_TYPE,
        HITS,
        SESSIONS,
        SESSIONS_PER_USER,
        AVG_SESSION_DURATION,
        BOUNCES,
        SESSION_DURATION,
        BOUNCE_RATE,

        
        '{{ schema_nm }}' as Source_type,
        'D_SESSION_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ schema_nm }}.SESSION
          {% if not loop.last %}
            UNION ALL
        {% endif %} 
        {% elif  entity_typ == 'X'  %} 
        select
        null as DATE,
        null as PROFILE,
        null as SESSION_DURATION_BUCKET,
        null as USER_TYPE,
        null as HITS,
        null as SESSIONS,
        null as SESSIONS_PER_USER,
        null as AVG_SESSION_DURATION,
        null as BOUNCES,
        null as SESSION_DURATION,
        null as BOUNCE_RATE
        from dual       
    {% endif %}
{% endfor %}