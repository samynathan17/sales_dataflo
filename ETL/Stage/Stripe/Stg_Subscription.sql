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
        unique_key= 'SUBSCRIPTION_ID',
        post_hook="DELETE FROM " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".STG_SUBSCRIPTION WHERE SUBSCRIPTION_ID IS NULL"
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
        APPLICATION_FEE_PERCENT,
        BILLING,
        BILLING_CYCLE_ANCHOR,
        BILLING_THRESHOLD_RESET_BILLING_CYCLE_ANCHOR,
        BILLING_THRESHOLD_AMOUNT_GTE,
        CANCEL_AT,
        CANCEL_AT_PERIOD_END,
        CANCELED_AT,
        CREATED,
        CURRENT_PERIOD_END,
        CURRENT_PERIOD_START,
        DAYS_UNTIL_DUE,
        ENDED_AT,
        LIVEMODE,
        QUANTITY,
        START_DATE,
        STATUS,
        TAX_PERCENT,
        TRIAL_START,
        TRIAL_END,
        CUSTOMER_ID,
        DEFAULT_SOURCE_ID,
        METADATA,
        _FIVETRAN_SYNCED,
        '{{ schema_nm }}' as Entity_type,
        'D_SUBSCRIPTION_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ schema_nm }}.SUBSCRIPTION
         {% if not loop.last %}
            UNION ALL
        {% endif %}
    {% elif  V_SF_Schema[0:1] == 'X'  %}     
       select
        null as INVOICE_LINE_ITEM_ID,
        null as SOURCE_ID,
        null as APPLICATION_FEE_PERCENT,
        null as BILLING,
        null as BILLING_CYCLE_ANCHOR,
        null as BILLING_THRESHOLD_RESET_BILLING_CYCLE_ANCHOR,
        null as BILLING_THRESHOLD_AMOUNT_GTE,
        null as CANCEL_AT,
        null as CANCEL_AT_PERIOD_END,
        null as CANCELED_AT,
        null as CREATED,
        null as CURRENT_PERIOD_END,
        null as CURRENT_PERIOD_START,
        null as DAYS_UNTIL_DUE,
        null as ENDED_AT,
        null as LIVEMODE,
        null as QUANTITY,
        null as START_DATE,
        null as STATUS,
        null as TAX_PERCENT,
        null as TRIAL_START,
        null as TRIAL_END,
        null as CUSTOMER_ID,
        null as DEFAULT_SOURCE_ID,
        null as METADATA,
        null as _FIVETRAN_SYNCED,
        null as Entity_type,
        null AS DW_SESSION_NM,
        null AS DW_INS_UPD_DTS
    FROM dual    
    {% endif %}
{% endfor %}