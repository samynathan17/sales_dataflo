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
        post_hook="DELETE FROM " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".STG_SOCIAL_MEDIA_ACQUISITIONS WHERE ID IS NULL"
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
        SOCIAL_NETWORK,
        SESSIONS,
        NEW_USERS,
        AVG_SESSION_DURATION,
        TRANSACTION_REVENUE,
        PAGEVIEWS_PER_SESSION,
        TRANSACTIONS,
        BOUNCE_RATE,
        PAGEVIEWS,
        PERCENT_NEW_SESSIONS,
        TRANSACTIONS_PER_SESSION,
       
        '{{ schema_nm }}' as Source_type,
        'D_SOCIAL_MEDIA_ACQUISITIONS_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ schema_nm }}.SOCIAL_MEDIA_ACQUISITIONS
          {% if not loop.last %}
            UNION ALL
        {% endif %} 
        {% elif  entity_typ == 'X'  %}
        select
        null as DATE,
        null as PROFILE,
        null as SOCIAL_NETWORK,
        null as SESSIONS,
        null as NEW_USERS,
        null as AVG_SESSION_DURATION,
        null as TRANSACTION_REVENUE,
        null as PAGEVIEWS_PER_SESSION,
        null as TRANSACTIONS,
        null as BOUNCE_RATE,
        null as PAGEVIEWS,
        null as PERCENT_NEW_SESSIONS,
        null as TRANSACTIONS_PER_SESSION
        from dual        
    {% endif %}
{% endfor %}