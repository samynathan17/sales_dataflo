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
        unique_key= 'BALANCE_TRANSACTION_ID',
        post_hook="DELETE FROM " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".STG_BALANCE_TRANSACTION WHERE BALANCE_TRANSACTION_ID IS NULL"
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
        {{ dbt_utils.surrogate_key('id') }}  AS BALANCE_TRANSACTION_ID,
        ID as Source_ID,
        CONNECTED_ACCOUNT_ID,
        AMOUNT,
        AVAILABLE_ON,
        CREATED,
        CURRENCY,
        DESCRIPTION,
        EXCHANGE_RATE,
        FEE,
        NET,
        SOURCE,
        STATUS,
        TYPE,
        PAYOUT_ID,
        _FIVETRAN_SYNCED,
        '{{ schema_nm }}' as Entity_type,
        'D_PAYOUT_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ schema_nm }}.BALANCE_TRANSACTION
         {% if not loop.last %}
            UNION ALL
        {% endif %}
    {% elif  V_SF_Schema[0:1] == 'X'  %}     
       select
        null as BALANCE_TRANSACTION_ID,
        null as Source_ID,
        null as CONNECTED_ACCOUNT_ID,
        null as AMOUNT,
        null as AVAILABLE_ON,
        null as CREATED,
        null as CURRENCY,
        null as DESCRIPTION,
        null as EXCHANGE_RATE,
        null as FEE,
        null as NET,
        null as SOURCE,
        null as STATUS,
        null as TYPE,
        null as PAYOUT_ID,
        null as _FIVETRAN_SYNCED,
        null as Entity_type,
        null AS DW_SESSION_NM,
        null AS DW_INS_UPD_DTS
    FROM dual    
    {% endif %}
{% endfor %}