{% set results = get_column_values_from_query("select * from " ~ var('V_DB') ~ "." ~ var('V_Entity_Schema')~ "." ~ var('V_Sales')~" where DATASOURCE_TYPE = 'STR' and READY_TO_PROCESS = 'TRUE'", "ENTITY_DATASORUCE_NAME||'#'||DATASOURCE_TYPE")%}

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
        unique_key= 'PLAN_ID',
        post_hook="DELETE FROM " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".STG_PLAN WHERE PLAN_ID IS NULL"
      )
}}


{% for V_SF_Schema in results %}

{% if V_SF_Schema != 'X' %} 
{% set schema_nm,entity_typ = V_SF_Schema.split('#') %}
{% else %}
{% set entity_typ = 'X' %}
{% endif %}

 {% if  entity_typ == 'STR'  %}   
      
  select
        {{ dbt_utils.surrogate_key('ID') }}  AS PLAN_ID,
        ID as SOURCE_ID,
        ACTIVE,
        AGGREGATE_USAGE,
        AMOUNT,
        BILLING_SCHEME,
        CREATED,
        CURRENCY,
        INTERVAL,
        INTERVAL_COUNT,
        LIVEMODE,
        NICKNAME,
        TIERS_MODE,
        TRANSFORM_USAGE_DIVIDE_BY,
        TRANSFORM_USAGE_ROUND,
        TRIAL_PERIOD_DAYS,
        USAGE_TYPE,
        IS_DELETED,
        METADATA,
        PRODUCT_ID,
        _FIVETRAN_SYNCED,
        '{{ schema_nm }}' as Entity_type,
        'D_PLAN_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ schema_nm }}.PLAN
         {% if not loop.last %}
            UNION ALL
        {% endif %}
    {% elif  V_SF_Schema[0:1] == 'X'  %}     
       select
        null as PLAN_ID,
        null as SOURCE_ID,
        null as ACTIVE,
        null as AGGREGATE_USAGE,
        null as AMOUNT,
        null as BILLING_SCHEME,
        null as CREATED,
        null as CURRENCY,
        null as INTERVAL,
        null as INTERVAL_COUNT,
        null as LIVEMODE,
        null as NICKNAME,
        null as TIERS_MODE,
        null as TRANSFORM_USAGE_DIVIDE_BY,
        null as TRANSFORM_USAGE_ROUND,
        null as TRIAL_PERIOD_DAYS,
        null as USAGE_TYPE,
        null as IS_DELETED,
        null as METADATA,
        null as PRODUCT_ID,
        null as _FIVETRAN_SYNCED,
        null as Entity_type,
        null AS DW_SESSION_NM,
        null AS DW_INS_UPD_DTS
    FROM dual    
    {% endif %}
{% endfor %}