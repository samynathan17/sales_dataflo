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
        unique_key= 'INVOICE_ID',
        post_hook="DELETE FROM " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".STG_INVOICE WHERE INVOICE_ID IS NULL"
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
        {{ dbt_utils.surrogate_key('ID') }}  AS INVOICE_ID,
        ID as SOURCE_ID,
        AMOUNT_DUE,
        AMOUNT_PAID,
        AMOUNT_REMAINING,
        ATTEMPT_COUNT,
        ATTEMPTED,
        AUTO_ADVANCE,
        BILLING,
        BILLING_REASON,
        CURRENCY,
        CREATED,
        DATE,
        DESCRIPTION,
        DUE_DATE,
        ENDING_BALANCE,
        FINALIZED_AT,
        FOOTER,
        HOSTED_INVOICE_URL,
        INVOICE_PDF,
        LIVEMODE,
        NEXT_PAYMENT_ATTEMPT,
        NUMBER,
        PAID,
        PERIOD_START,
        PERIOD_END,
        RECEIPT_NUMBER,
        STARTING_BALANCE,
        STATEMENT_DESCRIPTOR,
        STATUS,
        SUBSCRIPTION_PRORATION_DATE,
        SUBTOTAL,
        TAX,
        TAX_PERCENT,
        THRESHOLD_REASON_AMOUNT_GTE,
        STATUS_TRANSITIONS_FINALIZED_AT,
        STATUS_TRANSITIONS_PAID_AT,
        STATUS_TRANSITIONS_VOIDED_AT,
        STATUS_TRANSITIONS_MARKED_UNCOLLECTIBLE_AT,
        TOTAL,
        WEBHOOKS_DELIVERED_AT,
        IS_DELETED,
        APPLICATION_FEE_AMOUNT,
        CHARGE_ID,
        CUSTOMER_ID,
        DEFAULT_SOURCE_ID,
        METADATA,
        _FIVETRAN_SYNCED,
        '{{ schema_nm }}' as Entity_type,
        'D_INVOICE_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ schema_nm }}.INVOICE
         {% if not loop.last %}
            UNION ALL
        {% endif %}
    {% elif  V_SF_Schema[0:1] == 'X'  %}     
       select
        null as INVOICE_ID,
        null as SOURCE_ID,
        null as AMOUNT_DUE,
        null as AMOUNT_PAID,
        null as AMOUNT_REMAINING,
        null as ATTEMPT_COUNT,
        null as ATTEMPTED,
        null as AUTO_ADVANCE,
        null as BILLING,
        null as BILLING_REASON,
        null as CURRENCY,
        null as CREATED,
        null as DATE,
        null as DESCRIPTION,
        null as DUE_DATE,
        null as ENDING_BALANCE,
        null as FINALIZED_AT,
        null as FOOTER,
        null as HOSTED_INVOICE_URL,
        null as INVOICE_PDF,
        null as LIVEMODE,
        null as NEXT_PAYMENT_ATTEMPT,
        null as NUMBER,
        null as PAID,
        null as PERIOD_START,
        null as PERIOD_END,
        null as RECEIPT_NUMBER,
        null as STARTING_BALANCE,
        null as STATEMENT_DESCRIPTOR,
        null as STATUS,
        null as SUBSCRIPTION_PRORATION_DATE,
        null as SUBTOTAL,
        null as TAX,
        null as TAX_PERCENT,
        null as THRESHOLD_REASON_AMOUNT_GTE,
        null as STATUS_TRANSITIONS_FINALIZED_AT,
        null as STATUS_TRANSITIONS_PAID_AT,
        null as STATUS_TRANSITIONS_VOIDED_AT,
        null as STATUS_TRANSITIONS_MARKED_UNCOLLECTIBLE_AT,
        null as TOTAL,
        null as WEBHOOKS_DELIVERED_AT,
        null as IS_DELETED,
        null as APPLICATION_FEE_AMOUNT,
        null as CHARGE_ID,
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