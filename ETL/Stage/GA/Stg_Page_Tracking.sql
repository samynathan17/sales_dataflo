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
        post_hook="DELETE FROM " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".STG_PAGE_TRACKING WHERE ID IS NULL"
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
        PAGE_TITLE,
        LANDING_PAGE_PATH,
        PAGE_PATH,
        EXIT_PAGE_PATH,
        PAGE_VALUE,
        EXIT_RATE,
        TIME_ON_PAGE,
        PAGEVIEWS_PER_SESSION,
        UNIQUE_PAGEVIEWS,
        ENTRANCE_RATE,

        
        '{{ schema_nm }}' as Source_type,
        'D_PAGE_TRACKING_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ schema_nm }}.PAGE_TRACKING
          {% if not loop.last %}
            UNION ALL
        {% endif %}  
        {% elif  entity_typ == 'X'  %} 
        select
        null as DATE,
        null as PROFILE,
        null as PAGE_TITLE,
        null as LANDING_PAGE_PATH,
        null as PAGE_PATH,
        null as EXIT_PAGE_PATH,
        null as PAGE_VALUE,
        null as EXIT_RATE,
        null as TIME_ON_PAGE,
        null as PAGEVIEWS_PER_SESSION,
        null as UNIQUE_PAGEVIEWS,
        null as ENTRANCE_RATE
        from dual      
    {% endif %}
{% endfor %}