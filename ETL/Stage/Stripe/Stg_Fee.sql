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
        unique_key= 'FEE_ID',
        post_hook="DELETE FROM " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".STG_FEE WHERE FEE_ID IS NULL"
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
        {{ dbt_utils.surrogate_key('BALANCE_TRANSACTION_ID') }}  AS FEE_ID,
        BALANCE_TRANSACTION_ID,
        INDEX,
        CONNECTED_ACCOUNT_ID,
        AMOUNT,
        APPLICATION,
        CURRENCY,
        DESCRIPTION,
        TYPE,
        _FIVETRAN_SYNCED,
        '{{ schema_nm }}' as Entity_type,
        'D_FEE_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ schema_nm }}.FEE
         {% if not loop.last %}
            UNION ALL
        {% endif %}
    {% elif  V_SF_Schema[0:1] == 'X'  %}     
       select
        null as FEE_ID,
        null as BALANCE_TRANSACTION_ID,
        null as INDEX,
        null as CONNECTED_ACCOUNT_ID,
        null as AMOUNT,
        null as APPLICATION,
        null as CURRENCY,
        null as DESCRIPTION,
        null as TYPE,
        null as _FIVETRAN_SYNCED,
        null as Entity_type,
        null AS DW_SESSION_NM,
        null AS DW_INS_UPD_DTS
    FROM dual    
    {% endif %}
{% endfor %}