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
        {{ dbt_utils.surrogate_key('ID') }}  AS AGH_ID,
       BIDDING_STRATEGY_MAX_CPC_BID_FLOOR,
AD_GROUP_TYPE,
BIDDING_STRATEGY_CPC_BID_AMOUNT,
UPDATED_AT,
BASE_CAMPAIGN_ID,
BIDDING_STRATEGY_ID,
CAMPAIGN_ID,
ID,
BIDDING_STRATEGY_COMPETITOR_DOMAIN,
FINAL_URL_SUFFIX,
BIDDING_STRATEGY_BID_MODIFIER,
BASE_AD_GROUP_ID,
_FIVETRAN_SYNCED,
BIDDING_STRATEGY_ENHANCED_CPC_ENABLED,
BIDDING_STRATEGY_RAISE_BID_WHEN_LOW_QUALITY_SCORE,
BIDDING_STRATEGY_CPM_BID_AMOUNT,
BIDDING_STRATEGY_BID_FLOOR,
BIDDING_STRATEGY_BID_CEILING,
BIDDING_STRATEGY_BID_CHANGES_FOR_RAISES_ONLY,
BIDDING_STRATEGY_RAISE_BID_WHEN_BUDGET_CONSTRAINED,
BIDDING_STRATEGY_TARGET_OUTRANK_SHARE,
BIDDING_STRATEGY_CPA_BID_AMOUNT,
NAME,
TRACKING_URL_TEMPLATE,
BIDDING_STRATEGY_TARGET_ROAS,
BIDDING_STRATEGY_TARGET_CPA,
BIDDING_STRATEGY_VIEWABLE_CPM_ENABLED,
CONTENT_BID_CRITERION_TYPE_GROUP,
STATUS,
BIDDING_STRATEGY_MAX_CPC_BID_CEILING,
BIDDING_STRATEGY_STRATEGY_GOAL,
BIDDING_STRATEGY_SPEND_TARGET,
AD_GROUP_ROTATION_MODE,
CAMPAIGN_NAME,
BIDDING_STRATEGY_SCHEME_TYPE,
BIDDING_STRATEGY_TARGET_ROAS_OVERRIDE,
BIDDING_STRATEGY_SOURCE,
BIDDING_STRATEGY_TYPE,
BIDDING_STRATEGY_NAME,

        '{{ schema_nm }}' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ schema_nm }}.AD_GROUP_HISTORY
          {% if not loop.last %}
            UNION ALL
        {% endif %} 
        {% elif  entity_typ == 'X'  %} 
         select
        Null As BIDDING_STRATEGY_MAX_CPC_BID_FLOOR,
Null As AD_GROUP_TYPE,
Null As BIDDING_STRATEGY_CPC_BID_AMOUNT,
Null As UPDATED_AT,
Null As BASE_CAMPAIGN_ID,
Null As BIDDING_STRATEGY_ID,
Null As CAMPAIGN_ID,
Null As ID,
Null As BIDDING_STRATEGY_COMPETITOR_DOMAIN,
Null As FINAL_URL_SUFFIX,
Null As BIDDING_STRATEGY_BID_MODIFIER,
Null As BASE_AD_GROUP_ID,
Null As _FIVETRAN_SYNCED,
Null As BIDDING_STRATEGY_ENHANCED_CPC_ENABLED,
Null As BIDDING_STRATEGY_RAISE_BID_WHEN_LOW_QUALITY_SCORE,
Null As BIDDING_STRATEGY_CPM_BID_AMOUNT,
Null As BIDDING_STRATEGY_BID_FLOOR,
Null As BIDDING_STRATEGY_BID_CEILING,
Null As BIDDING_STRATEGY_BID_CHANGES_FOR_RAISES_ONLY,
Null As BIDDING_STRATEGY_RAISE_BID_WHEN_BUDGET_CONSTRAINED,
Null As BIDDING_STRATEGY_TARGET_OUTRANK_SHARE,
Null As BIDDING_STRATEGY_CPA_BID_AMOUNT,
Null As NAME,
Null As TRACKING_URL_TEMPLATE,
Null As BIDDING_STRATEGY_TARGET_ROAS,
Null As BIDDING_STRATEGY_TARGET_CPA,
Null As BIDDING_STRATEGY_VIEWABLE_CPM_ENABLED,
Null As CONTENT_BID_CRITERION_TYPE_GROUP,
Null As STATUS,
Null As BIDDING_STRATEGY_MAX_CPC_BID_CEILING,
Null As BIDDING_STRATEGY_STRATEGY_GOAL,
Null As BIDDING_STRATEGY_SPEND_TARGET,
Null As AD_GROUP_ROTATION_MODE,
Null As CAMPAIGN_NAME,
Null As BIDDING_STRATEGY_SCHEME_TYPE,
Null As BIDDING_STRATEGY_TARGET_ROAS_OVERRIDE,
Null As BIDDING_STRATEGY_SOURCE,
Null As BIDDING_STRATEGY_TYPE,
Null As BIDDING_STRATEGY_NAME,

        '{{ schema_nm }}' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM dual     

    {% endif %}
{% endfor %}