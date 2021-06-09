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
        unique_key= 'PAYMENT_INTENT_ID',
        post_hook="DELETE FROM " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".STG_PAYMENT_INTENT WHERE PAYMENT_INTENT_ID IS NULL"
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
        {{ dbt_utils.surrogate_key('ID') }}  AS PAYMENT_INTENT_ID,
        ID as SOURCE_PAYMENT_ID,
        AMOUNT,
        AMOUNT_CAPTURABLE,
        AMOUNT_RECEIVED,
        APPLICATION,
        APPLICATION_FEE_AMOUNT,
        CANCELED_AT,
        CANCELLATION_REASON,
        CAPTURE_METHOD,
        CONFIRMATION_METHOD,
        CREATED,
        CURRENCY,
        DESCRIPTION,
        LAST_PAYMENT_ERROR_TYPE,
        LAST_PAYMENT_ERROR_CODE,
        LAST_PAYMENT_ERROR_DECLINE_CODE,
        LAST_PAYMENT_ERROR_DOC_URL,
        LAST_PAYMENT_ERROR_MESSAGE,
        LAST_PAYMENT_ERROR_PARAM,
        LAST_PAYMENT_ERROR_SOURCE_ID,
        LAST_PAYMENT_ERROR_CHARGE_ID,
        LIVEMODE,
        ON_BEHALF_OF,
        RECEIPT_EMAIL,
        STATEMENT_DESCRIPTOR,
        STATUS,
        TRANSFER_DATA_DESTINATION,
        TRANSFER_GROUP,
        PAYMENT_METHOD_ID,
        CUSTOMER_ID,
        METADATA,
        SOURCE_ID,
        _FIVETRAN_SYNCED,
        '{{ schema_nm }}' as Entity_type,
        'D_PAYMENT_INTENT_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ schema_nm }}.PAYMENT_INTENT
         {% if not loop.last %}
            UNION ALL
        {% endif %}
    {% elif  V_SF_Schema[0:1] == 'X'  %}     
       select
        null as PAYMENT_INTENT_ID,
        null as SOURCE_PAYMENT_ID,
        null as AMOUNT,
        null as AMOUNT_CAPTURABLE,
        null as AMOUNT_RECEIVED,
        null as APPLICATION,
        null as APPLICATION_FEE_AMOUNT,
        null as CANCELED_AT,
        null as CANCELLATION_REASON,
        null as CAPTURE_METHOD,
        null as CONFIRMATION_METHOD,
        null as CREATED,
        null as CURRENCY,
        null as DESCRIPTION,
        null as LAST_PAYMENT_ERROR_TYPE,
        null as LAST_PAYMENT_ERROR_CODE,
        null as LAST_PAYMENT_ERROR_DECLINE_CODE,
        null as LAST_PAYMENT_ERROR_DOC_URL,
        null as LAST_PAYMENT_ERROR_MESSAGE,
        null as LAST_PAYMENT_ERROR_PARAM,
        null as LAST_PAYMENT_ERROR_SOURCE_ID,
        null as LAST_PAYMENT_ERROR_CHARGE_ID,
        null as LIVEMODE,
        null as ON_BEHALF_OF,
        null as RECEIPT_EMAIL,
        null as STATEMENT_DESCRIPTOR,
        null as STATUS,
        null as TRANSFER_DATA_DESTINATION,
        null as TRANSFER_GROUP,
        null as PAYMENT_METHOD_ID,
        null as CUSTOMER_ID,
        null as METADATA,
        null as SOURCE_ID,
        null as _FIVETRAN_SYNCED,
        null as Entity_type,
        null AS DW_SESSION_NM,
        null AS DW_INS_UPD_DTS
    FROM dual    
    {% endif %}
{% endfor %}