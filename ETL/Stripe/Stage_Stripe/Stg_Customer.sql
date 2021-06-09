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
        unique_key= 'CUSTOMER_ID',
        post_hook="DELETE FROM " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".STG_CUSTOMER WHERE CUSTOMER_ID IS NULL"
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
        {{ dbt_utils.surrogate_key('ID') }}  AS CUSTOMER_ID,
        ID as SOURCE_CUST_ID,
        ACCOUNT_BALANCE,
        BALANCE,
        CREATED,
        CURRENCY,
        ADDRESS_CITY,
        ADDRESS_COUNTRY,
        ADDRESS_LINE_1,
        ADDRESS_LINE_2,
        ADDRESS_POSTAL_CODE,
        ADDRESS_STATE,
        NAME,
        BANK_ACCOUNT_ID,
        SOURCE_ID,
        DEFAULT_CARD_ID,
        DELINQUENT,
        DESCRIPTION,
        EMAIL,
        PHONE,
        INVOICE_PREFIX,
        INVOICE_SETTINGS_DEFAULT_PAYMENT_METHOD,
        INVOICE_SETTINGS_FOOTER,
        LIVEMODE,
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
        TAX_INFO_TAX_ID,
        TAX_INFO_TYPE,
        TAX_EXEMPT,
        TAX_INFO_VERIFICATION_STATUS,
        TAX_INFO_VERIFICATION_VERIFIED_NAME,
        IS_DELETED,
        METADATA,
        _FIVETRAN_SYNCED,
        '{{ schema_nm }}' as Entity_type,
        'D_CUSTOMER_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ schema_nm }}.CUSTOMER
         {% if not loop.last %}
            UNION ALL
        {% endif %}
    {% elif  V_SF_Schema[0:1] == 'X'  %}     
       select
        null as CUSTOMER_ID,
        null as SOURCE_CUST_ID,
        null as ACCOUNT_BALANCE,
        null as BALANCE,
        null as CREATED,
        null as CURRENCY,
        null as ADDRESS_CITY,
        null as ADDRESS_COUNTRY,
        null as ADDRESS_LINE_1,
        null as ADDRESS_LINE_2,
        null as ADDRESS_POSTAL_CODE,
        null as ADDRESS_STATE,
        null as NAME,
        null as BANK_ACCOUNT_ID,
        null as SOURCE_ID,
        null as DEFAULT_CARD_ID,
        null as DELINQUENT,
        null as DESCRIPTION,
        null as EMAIL,
        null as PHONE,
        null as INVOICE_PREFIX,
        null as INVOICE_SETTINGS_DEFAULT_PAYMENT_METHOD,
        null as INVOICE_SETTINGS_FOOTER,
        null as LIVEMODE,
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
        null as TAX_INFO_TAX_ID,
        null as TAX_INFO_TYPE,
        null as TAX_EXEMPT,
        null as TAX_INFO_VERIFICATION_STATUS,
        null as TAX_INFO_VERIFICATION_VERIFIED_NAME,
        null as IS_DELETED,
        null as METADATA,
        null as _FIVETRAN_SYNCED,
        null as Entity_type,
        null AS DW_SESSION_NM,
        null AS DW_INS_UPD_DTS
    FROM dual    
    {% endif %}
{% endfor %}
