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
       CAMPAIGN_STATUS,
_FIVETRAN_SYNCED,
ACCOUNT_DESCRIPTIVE_NAME,
_FIVETRAN_ID,
AD_GROUP_STATUS,
AD_GROUP_NAME,
DATE,
CAMPAIGN_ID,
EXTERNAL_CUSTOMER_ID,
CRITERIA,
AD_GROUP_ID,
CUSTOMER_ID,
ID,
CRITERIA_TYPE,
COST,
CAMPAIGN_NAME,
IMPRESSIONS,
CLICKS,
CRITERIA_DESTINATION_URL,

        '{{ schema_nm }}' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ schema_nm }}.CRITERIA_PERFORMANCE
          {% if not loop.last %}
            UNION ALL
        {% endif %} 
        {% elif  entity_typ == 'X'  %} 
         select
      Null As CAMPAIGN_STATUS,
Null As _FIVETRAN_SYNCED,
Null As ACCOUNT_DESCRIPTIVE_NAME,
Null As _FIVETRAN_ID,
Null As AD_GROUP_STATUS,
Null As AD_GROUP_NAME,
Null As DATE,
Null As CAMPAIGN_ID,
Null As EXTERNAL_CUSTOMER_ID,
Null As CRITERIA,
Null As AD_GROUP_ID,
Null As CUSTOMER_ID,
Null As ID,
Null As CRITERIA_TYPE,
Null As COST,
Null As CAMPAIGN_NAME,
Null As IMPRESSIONS,
Null As CLICKS,
Null As CRITERIA_DESTINATION_URL,

        '{{ schema_nm }}' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM dual     

    {% endif %}
{% endfor %}