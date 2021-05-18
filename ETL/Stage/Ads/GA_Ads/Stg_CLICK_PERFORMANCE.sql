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
       AD_GROUP_ID,

CLICKS,
EXTERNAL_CUSTOMER_ID,
AD_GROUP_STATUS,
CAMPAIGN_STATUS,
ACCOUNT_DESCRIPTIVE_NAME,
GCL_ID,
AD_GROUP_NAME,
CAMPAIGN_ID,
_FIVETRAN_ID,
CRITERIA_ID,
CAMPAIGN_NAME,
DATE,

        '{{ schema_nm }}' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ schema_nm }}.CLICK_PERFORMANCE
          {% if not loop.last %}
            UNION ALL
        {% endif %} 
        {% elif  entity_typ == 'X'  %} 
         select
       Null As AD_GROUP_ID,
Null As _FIVETRAN_SYNCED,
Null As CUSTOMER_ID,
Null As CLICKS,
Null As EXTERNAL_CUSTOMER_ID,
Null As AD_GROUP_STATUS,
Null As CAMPAIGN_STATUS,
Null As ACCOUNT_DESCRIPTIVE_NAME,
Null As GCL_ID,
Null As AD_GROUP_NAME,
Null As CAMPAIGN_ID,
Null As _FIVETRAN_ID,
Null As CRITERIA_ID,
Null As CAMPAIGN_NAME,
Null As DATE,

        '{{ schema_nm }}' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM dual     

    {% endif %}
{% endfor %}