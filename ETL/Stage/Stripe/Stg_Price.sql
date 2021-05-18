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
        unique_key= 'PRICE_ID',
        post_hook="DELETE FROM " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".STG_PRICE WHERE PRICE_ID IS NULL"
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
        {{ dbt_utils.surrogate_key('ID') }}  AS PRICE_ID,
        ID as SOURCE_ID,
        ACTIVE,
        CURRENCY,
        NICKNAME,
        RECURRING_AGGREGATE_USAGE,
        RECURRING_INTERVAL,
        RECURRING_INTERVAL_COUNT,
        RECURRING_USAGE_TYPE,
        TYPE,
        UNIT_AMOUNT,
        BILLING_SCHEME,
        CREATED,
        LIVEMODE,
        LOOKUP_KEY,
        TIERS_MODE,
        TRANSFORM_QUANTITY_DIVIDE_BY,
        TRANSFORM_QUANTITY_ROUND,
        UNIT_AMOUNT_DECIMAL,
        IS_DELETED,
        PRODUCT_ID,
        _FIVETRAN_SYNCED,
        '{{ schema_nm }}' as Entity_type,
        'D_PRICE_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ schema_nm }}.PRICE
         {% if not loop.last %}
            UNION ALL
        {% endif %}
    {% elif  V_SF_Schema[0:1] == 'X'  %}     
       select
        null as PRICE_ID,
        null as SOURCE_ID,
        null as ACTIVE,
        null as CURRENCY,
        null as NICKNAME,
        null as RECURRING_AGGREGATE_USAGE,
        null as RECURRING_INTERVAL,
        null as RECURRING_INTERVAL_COUNT,
        null as RECURRING_USAGE_TYPE,
        null as TYPE,
        null as UNIT_AMOUNT,
        null as BILLING_SCHEME,
        null as CREATED,
        null as LIVEMODE,
        null as LOOKUP_KEY,
        null as TIERS_MODE,
        null as TRANSFORM_QUANTITY_DIVIDE_BY,
        null as TRANSFORM_QUANTITY_ROUND,
        null as UNIT_AMOUNT_DECIMAL,
        null as IS_DELETED,
        null as PRODUCT_ID,
        null as _FIVETRAN_SYNCED,
        null as Entity_type,
        null AS DW_SESSION_NM,
        null AS DW_INS_UPD_DTS
    FROM dual    
    {% endif %}
{% endfor %}