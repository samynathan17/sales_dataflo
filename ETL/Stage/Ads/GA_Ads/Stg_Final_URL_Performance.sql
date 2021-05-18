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
        {{ dbt_utils.surrogate_key('_FIVETRAN_ID') }}  AS ACC_ID,
        IMPRESSIONS,
        _FIVETRAN_SYNCED,
        EFFECTIVE_FINAL_URL,
        CUSTOMER_ID,
        ACCOUNT_DESCRIPTIVE_NAME account_name,
        COST,
        CAMPAIGN_STATUS,
        CLICKS,
        AD_GROUP_STATUS,
        AD_GROUP_NAME,
        DATE,
        CAMPAIGN_NAME,
        _FIVETRAN_ID,
        CAMPAIGN_ID,
        AD_GROUP_ID,
        EXTERNAL_CUSTOMER_ID,
        '{{ schema_nm }}' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ schema_nm }}.FINAL_URL_PERFORMANCE
          {% if not loop.last %}
            UNION ALL
        {% endif %} 
        {% elif  entity_typ == 'X'  %} 
         select
       Null As IMPRESSIONS,
Null As _FIVETRAN_SYNCED,
Null As EFFECTIVE_FINAL_URL,
Null As CUSTOMER_ID,
Null As ACCOUNT_DESCRIPTIVE_NAME,
Null As COST,
Null As CAMPAIGN_STATUS,
Null As CLICKS,
Null As AD_GROUP_STATUS,
Null As AD_GROUP_NAME,
Null As DATE,
Null As CAMPAIGN_NAME,
Null As _FIVETRAN_ID,
Null As CAMPAIGN_ID,
Null As AD_GROUP_ID,
Null As EXTERNAL_CUSTOMER_ID,

        '{{ schema_nm }}' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM dual     

    {% endif %}
{% endfor %}