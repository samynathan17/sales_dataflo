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
        unique_key= 'INVOICE_LINE_ITEM_ID',
        post_hook="DELETE FROM " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".STG_INVOICE_LINE_ITEM WHERE INVOICE_LINE_ITEM_ID IS NULL"
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
        {{ dbt_utils.surrogate_key('ID') }}  AS INVOICE_LINE_ITEM_ID,
        ID as SOURCE_ID,
        INVOICE_ID,
        UNIQUE_ID,
        AMOUNT,
        CURRENCY,
        DESCRIPTION,
        DISCOUNTABLE,
        LIVEMODE,
        PERIOD_START,
        PERIOD_END,
        PRORATION,
        QUANTITY,
        TYPE,
        PLAN_ID,
        METADATA,
        SUBSCRIPTION_ID,
        SUBSCRIPTION_ITEM_ID,
        _FIVETRAN_SYNCED,
        '{{ schema_nm }}' as Entity_type,
        'D_INVOICE_ITEM_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ schema_nm }}.INVOICE_LINE_ITEM
         {% if not loop.last %}
            UNION ALL
        {% endif %}
    {% elif  V_SF_Schema[0:1] == 'X'  %}     
       select
        null as INVOICE_LINE_ITEM_ID,
        null as SOURCE_ID,
        null as INVOICE_ID,
        null as UNIQUE_ID,
        null as AMOUNT,
        null as CURRENCY,
        null as DESCRIPTION,
        null as DISCOUNTABLE,
        null as LIVEMODE,
        null as PERIOD_START,
        null as PERIOD_END,
        null as PRORATION,
        null as QUANTITY,
        null as TYPE,
        null as PLAN_ID,
        null as METADATA,
        null as SUBSCRIPTION_ID,
        null as SUBSCRIPTION_ITEM_ID,
        null as _FIVETRAN_SYNCED,
        null as Entity_type,
        null AS DW_SESSION_NM,
        null AS DW_INS_UPD_DTS
    FROM dual    
    {% endif %}
{% endfor %}