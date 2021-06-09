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
        post_hook="DELETE FROM " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".STG_GEO_NETWORK WHERE ID IS NULL"
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
        CONTINENT,
        COUNTRY,
        CITY,
        METRO,
        REGION,
        NETWORK_LOCATION,
        SESSIONS,
        USERS,

        
        '{{ schema_nm }}' as Source_type,
        'D_GEO_NETWORK_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ schema_nm }}.GEO_NETWORK
          {% if not loop.last %}
            UNION ALL
        {% endif %}        
    
    {% elif  entity_typ == 'X'  %}    
    select
        null as DATE,
        null as PROFILE,
        null as CONTINENT,
        null as COUNTRY,
        null as CITY,
        null as METRO,
        null as REGION,
        null as NETWORK_LOCATION,
        null as SESSIONS,
        null as USERS
        from dual
        {% endif %}

{% endfor %}