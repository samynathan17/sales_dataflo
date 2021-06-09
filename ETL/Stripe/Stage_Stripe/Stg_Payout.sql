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
        unique_key= 'PAYOUT_ID',
        post_hook="DELETE FROM " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".STG_PAYOUT WHERE PAYOUT_ID IS NULL"
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
        {{ dbt_utils.surrogate_key('id') }}  AS PAYOUT_ID,
        ID as Source_ID,
        CONNECTED_ACCOUNT_ID,
        AMOUNT,
        ARRIVAL_DATE,
        AUTOMATIC,
        CREATED,
        CURRENCY,
        DESCRIPTION,
        FAILURE_CODE,
        FAILURE_MESSAGE,
        LIVEMODE,
        METHOD,
        SOURCE_TYPE,
        STATEMENT_DESCRIPTOR,
        STATUS,
        TYPE,
        DESTINATION_BANK_ACCOUNT_ID,
        DESTINATION_CARD_ID,
        BALANCE_TRANSACTION_ID,
        FAILURE_BALANCE_TRANSACTION_ID,
        METADATA,
        _FIVETRAN_SYNCED,
        '{{ schema_nm }}' as Entity_type,
        'D_PAYOUT_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ schema_nm }}.Payout
         {% if not loop.last %}
            UNION ALL
        {% endif %}
    {% elif  V_SF_Schema[0:1] == 'X'  %}     
       select
        null as PAYOUT_ID,
        null as Source_ID,
        null as CONNECTED_ACCOUNT_ID,
        null as AMOUNT,
        null as ARRIVAL_DATE,
        null as AUTOMATIC,
        null as CREATED,
        null as CURRENCY,
        null as DESCRIPTION,
        null as FAILURE_CODE,
        null as FAILURE_MESSAGE,
        null as LIVEMODE,
        null as METHOD,
        null as SOURCE_TYPE,
        null as STATEMENT_DESCRIPTOR,
        null as STATUS,
        null as TYPE,
        null as DESTINATION_BANK_ACCOUNT_ID,
        null as DESTINATION_CARD_ID,
        null as BALANCE_TRANSACTION_ID,
        null as FAILURE_BALANCE_TRANSACTION_ID,
        null as METADATA,
        null as _FIVETRAN_SYNCED,
        null as Entity_type,
        null AS DW_SESSION_NM,
        null AS DW_INS_UPD_DTS
    FROM dual    
    {% endif %}
{% endfor %}