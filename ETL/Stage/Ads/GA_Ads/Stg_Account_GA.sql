{% set results = get_column_values_from_query("select * from " ~ var('V_DB') ~ "." ~ var('V_Entity_Schema')~ "." ~ var('V_Mkt')~" where DATASOURCE_TYPE = 'GA_ADS' and READY_TO_PROCESS = 'TRUE'", "ENTITY_DATASORUCE_NAME||'#'||DATASOURCE_TYPE")%}

{% if not var("enable_SF_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

{% for V_SF_Schema in results %}


{% if V_SF_Schema != 'X' %} 
{% set schema_nm,entity_typ = V_SF_Schema.split('#') %}
{% else %}
{% set entity_typ = 'X' %}
{% endif %}

 {% if  entity_typ == 'GA_ADS'  %} 
      
  select
        {{ dbt_utils.surrogate_key('ID') }}  AS ACC_ID,
        TEST_ACCOUNT,
        DATE_TIMEZONE,
        ID,
        ACCOUNT_LABEL_ID,
        ACCOUNT_LABEL_NAME,
        CURRENCY_CODE,
        _FIVETRAN_SYNCED,
        MANAGER_CUSTOMER_ID,
        SEQUENCE_ID,
        NAME,
        CAN_MANAGE_CLIENTS,
        '{{ schema_nm }}' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ schema_nm }}.ACCOUNT
          {% if not loop.last %}
            UNION ALL
        {% endif %} 
        {% elif  entity_typ == 'X'  %} 
         select
       Null As TEST_ACCOUNT,
Null As DATE_TIMEZONE,
Null As ID,
Null As ACCOUNT_LABEL_ID,
Null As ACCOUNT_LABEL_NAME,
Null As CURRENCY_CODE,
Null As _FIVETRAN_SYNCED,
Null As MANAGER_CUSTOMER_ID,
Null As SEQUENCE_ID,
Null As NAME,
Null As CAN_MANAGE_CLIENTS,
        '{{ schema_nm }}' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM dual     

    {% endif %}
{% endfor %}