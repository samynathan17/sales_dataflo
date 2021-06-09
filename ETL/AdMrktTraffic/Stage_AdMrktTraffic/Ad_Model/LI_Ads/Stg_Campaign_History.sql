{% set results = get_column_values_from_query("select * from " ~ var('V_DB') ~ "." ~ var('V_Entity_Schema')~ "." ~ var('V_Mkt')~" where DATASOURCE_TYPE = 'LI_ADS' and READY_TO_PROCESS = 'TRUE'", "ENTITY_DATASORUCE_NAME||'#'||DATASOURCE_TYPE")%}

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

 {% if  entity_typ == 'LI_ADS'  %} 
      
  select
        {{ dbt_utils.surrogate_key('ID') }}  AS CAMP_ID,
       ID,
LOCALE_LANGUAGE,
FORMAT,
LOCALE_COUNTRY,
LAST_MODIFIED_TIME,
OPTIMIZATION_TARGET_TYPE,
VERSION_TAG,
OFFSITE_DELIVERY_ENABLED,
DAILY_BUDGET_AMOUNT,
STATUS,
CREATED_TIME,
COST_TYPE,
RUN_SCHEDULE_START,
UNIT_COST_CURRENCY_CODE,
ASSOCIATED_ENTITY,
AUDIENCE_EXPANSION_ENABLED,
CAMPAIGN_GROUP_ID,
DAILY_BUDGET_CURRENCY_CODE,
UNIT_COST_AMOUNT,
TYPE,
ACCOUNT_ID,
CREATIVE_SELECTION,
NAME,
OBJECTIVE_TYPE,
RUN_SCHEDULE_END,

        '{{ schema_nm }}' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ schema_nm }}.CAMPAIGN_HISTORY
          {% if not loop.last %}
            UNION ALL
        {% endif %} 
        {% elif  entity_typ == 'X'  %} 
         select
       NULL AS ID,
NULL AS LOCALE_LANGUAGE,
NULL AS FORMAT,
NULL AS LOCALE_COUNTRY,
NULL AS LAST_MODIFIED_TIME,
NULL AS OPTIMIZATION_TARGET_TYPE,
NULL AS VERSION_TAG,
NULL AS OFFSITE_DELIVERY_ENABLED,
NULL AS DAILY_BUDGET_AMOUNT,
NULL AS STATUS,
NULL AS CREATED_TIME,
NULL AS COST_TYPE,
NULL AS RUN_SCHEDULE_START,
NULL AS UNIT_COST_CURRENCY_CODE,
NULL AS ASSOCIATED_ENTITY,
NULL AS AUDIENCE_EXPANSION_ENABLED,
NULL AS CAMPAIGN_GROUP_ID,
NULL AS DAILY_BUDGET_CURRENCY_CODE,
NULL AS UNIT_COST_AMOUNT,
NULL AS TYPE,
NULL AS ACCOUNT_ID,
NULL AS CREATIVE_SELECTION,
NULL AS NAME,
NULL AS OBJECTIVE_TYPE,
NULL AS RUN_SCHEDULE_END,

        '{{ schema_nm }}' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM dual     

    {% endif %}
{% endfor %}