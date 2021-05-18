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
        unique_key= 'CHARGE_ID',
        post_hook="DELETE FROM " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".STG_CHARGE WHERE CHARGE_ID IS NULL"
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
        {{ dbt_utils.surrogate_key('ID') }}  AS CHARGE_ID,
        ID as SOURCE_ID,
        CONNECTED_ACCOUNT_ID,
        AMOUNT,
        AMOUNT_REFUNDED,
        APPLICATION,
        APPLICATION_FEE_AMOUNT,
        CALCULATED_STATEMENT_DESCRIPTOR,
        CAPTURED,
        CREATED,
        CURRENCY,
        DESCRIPTION,
        DESTINATION,
        FAILURE_CODE,
        FAILURE_MESSAGE,
        FRAUD_DETAILS_USER_REPORT,
        FRAUD_DETAILS_STRIPE_REPORT,
        LIVEMODE,
        ON_BEHALF_OF,
        OUTCOME_NETWORK_STATUS,
        OUTCOME_REASON,
        OUTCOME_RISK_LEVEL,
        OUTCOME_RISK_SCORE,
        OUTCOME_SELLER_MESSAGE,
        OUTCOME_TYPE,
        PAID,
        RECEIPT_EMAIL,
        RECEIPT_NUMBER,
        RECEIPT_URL,
        REFUNDED,
        SHIPPING_ADDRESS_CITY,
        SHIPPING_ADDRESS_COUNTRY,
        SHIPPING_ADDRESS_LINE_1,
        SHIPPING_ADDRESS_LINE_2,
        SHIPPING_ADDRESS_POSTAL_CODE,
        SHIPPING_ADDRESS_STATE,
        SHIPPING_CARRIER,
        SHIPPING_NAME,
        SHIPPING_PHONE,
        SHIPPING_TRACKING_NUMBER,
        CARD_ID,
        BANK_ACCOUNT_ID,
        SOURCE_ID,
        SOURCE_TRANSFER,
        STATEMENT_DESCRIPTOR,
        STATUS,
        TRANSFER_DATA_DESTINATION,
        TRANSFER_GROUP,
        BALANCE_TRANSACTION_ID,
        CUSTOMER_ID,
        INVOICE_ID,
        METADATA,
        PAYMENT_INTENT_ID,
        PAYMENT_METHOD_ID,
        TRANSFER_ID,
        _FIVETRAN_SYNCED,
        '{{ schema_nm }}' as Entity_type,
        'D_CHARGE_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ schema_nm }}.CHARGE
         {% if not loop.last %}
            UNION ALL
        {% endif %}
    {% elif  V_SF_Schema[0:1] == 'X'  %}     
       select
        null as CHARGE_ID,
        null as SOURCE_ID,
        null as CONNECTED_ACCOUNT_ID,
        null as AMOUNT,
        null as AMOUNT_REFUNDED,
        null as APPLICATION,
        null as APPLICATION_FEE_AMOUNT,
        null as CALCULATED_STATEMENT_DESCRIPTOR,
        null as CAPTURED,
        null as CREATED,
        null as CURRENCY,
        null as DESCRIPTION,
        null as DESTINATION,
        null as FAILURE_CODE,
        null as FAILURE_MESSAGE,
        null as FRAUD_DETAILS_USER_REPORT,
        null as FRAUD_DETAILS_STRIPE_REPORT,
        null as LIVEMODE,
        null as ON_BEHALF_OF,
        null as OUTCOME_NETWORK_STATUS,
        null as OUTCOME_REASON,
        null as OUTCOME_RISK_LEVEL,
        null as OUTCOME_RISK_SCORE,
        null as OUTCOME_SELLER_MESSAGE,
        null as OUTCOME_TYPE,
        null as PAID,
        null as RECEIPT_EMAIL,
        null as RECEIPT_NUMBER,
        null as RECEIPT_URL,
        null as REFUNDED,
        null as SHIPPING_ADDRESS_CITY,
        null as SHIPPING_ADDRESS_COUNTRY,
        null as SHIPPING_ADDRESS_LINE_1,
        null as SHIPPING_ADDRESS_LINE_2,
        null as SHIPPING_ADDRESS_POSTAL_CODE,
        null as SHIPPING_ADDRESS_STATE,
        null as SHIPPING_CARRIER,
        null as SHIPPING_NAME,
        null as SHIPPING_PHONE,
        null as SHIPPING_TRACKING_NUMBER,
        null as CARD_ID,
        null as BANK_ACCOUNT_ID,
        null as SOURCE_ID,
        null as SOURCE_TRANSFER,
        null as STATEMENT_DESCRIPTOR,
        null as STATUS,
        null as TRANSFER_DATA_DESTINATION,
        null as TRANSFER_GROUP,
        null as BALANCE_TRANSACTION_ID,
        null as CUSTOMER_ID,
        null as INVOICE_ID,
        null as METADATA,
        null as PAYMENT_INTENT_ID,
        null as PAYMENT_METHOD_ID,
        null as TRANSFER_ID,
        null as _FIVETRAN_SYNCED,
        null as Entity_type,
        null AS DW_SESSION_NM,
        null AS DW_INS_UPD_DTS
    FROM dual    
    {% endif %}
{% endfor %}