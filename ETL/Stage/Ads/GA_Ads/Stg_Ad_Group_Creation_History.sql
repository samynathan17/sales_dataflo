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
        {{ dbt_utils.surrogate_key('ID') }}  AS AGCH_ID,
        BIDDING_STRATEGY_SCHEME_TYPE,
PRODUCT_CANONICAL_CONDITION,
BIDDING_STRATEGY_NAME,
ID,
PRODUCT_CHANNEL,
INCOME_RANGE_TYPE,
PRODUCT_CUSTOM_ATTRIBUTE_VALUE,
PRODUCT_TYPE,
BIDDING_STRATEGY_TARGET_ROAS,
PRODUCT_OFFER_ID,
PRODUCT_ADWORDS_LABEL,
SYSTEM_SERVING_STATUS,
USER_INTEREST_ID,
VIDEO_ID,
VIDEO_NAME,
BIDDING_STRATEGY_TARGET_CPA,
BID_MODIFIER,
AD_GROUP_ID,
BIDDING_STRATEGY_BID_CEILING,
CRITERION_USE,
CUSTOM_AFFINITY_ID,
BIDDING_STRATEGY_BID_MODIFIER,
PRODUCT_TYPE_FULL,
BIDDING_STRATEGY_TARGET_ROAS_OVERRIDE,
USER_INTEREST_PARENT_ID,
BIDDING_STRATEGY_RAISE_BID_WHEN_BUDGET_CONSTRAINED,
USER_INTEREST_NAME,
PRODUCT_LEGACY_CONDITION,
KEYWORD_MATCH_TYPE,
AD_GROUP_CRITERION_TYPE,
BIDDING_STRATEGY_TYPE,
PRODUCT_CUSTOM_ATTRIBUTE_TYPE,
USER_LIST_MEMBERSHIP_STATUS,
BIDDING_STRATEGY_COMPETITOR_DOMAIN,
BIDDING_STRATEGY_CPC_BID_AMOUNT,
TRACKING_URL_TEMPLATE,
PARTITION_TYPE,
PARENT_TYPE,
USER_LIST_ELIGIBLE_FOR_SEARCH,
BIDDING_STRATEGY_SOURCE,
BIDDING_STRATEGY_BID_FLOOR,
BIDDING_STRATEGY_MAX_CPC_BID_FLOOR,
CHANNEL_NAME,
CUSTOM_INTENT_ID,
APP_ID,

        '{{ schema_nm }}' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ schema_nm }}.AD_GROUP_CRITERION_HISTORY
          {% if not loop.last %}
            UNION ALL
        {% endif %} 
        {% elif  entity_typ == 'X'  %} 
         select
        Null As BIDDING_STRATEGY_SCHEME_TYPE,
Null As PRODUCT_CANONICAL_CONDITION,
Null As BIDDING_STRATEGY_NAME,
Null As ID,
Null As PRODUCT_CHANNEL,
Null As INCOME_RANGE_TYPE,
Null As PRODUCT_CUSTOM_ATTRIBUTE_VALUE,
Null As PRODUCT_TYPE,
Null As BIDDING_STRATEGY_TARGET_ROAS,
Null As PRODUCT_OFFER_ID,
Null As PRODUCT_ADWORDS_LABEL,
Null As SYSTEM_SERVING_STATUS,
Null As USER_INTEREST_ID,
Null As VIDEO_ID,
Null As VIDEO_NAME,
Null As BIDDING_STRATEGY_TARGET_CPA,
Null As BID_MODIFIER,
Null As AD_GROUP_ID,
Null As BIDDING_STRATEGY_BID_CEILING,
Null As CRITERION_USE,
Null As CUSTOM_AFFINITY_ID,
Null As BIDDING_STRATEGY_BID_MODIFIER,
Null As PRODUCT_TYPE_FULL,
Null As BIDDING_STRATEGY_TARGET_ROAS_OVERRIDE,
Null As USER_INTEREST_PARENT_ID,
Null As BIDDING_STRATEGY_RAISE_BID_WHEN_BUDGET_CONSTRAINED,
Null As USER_INTEREST_NAME,
Null As PRODUCT_LEGACY_CONDITION,
Null As KEYWORD_MATCH_TYPE,
Null As AD_GROUP_CRITERION_TYPE,
Null As BIDDING_STRATEGY_TYPE,
Null As PRODUCT_CUSTOM_ATTRIBUTE_TYPE,
Null As USER_LIST_MEMBERSHIP_STATUS,
Null As BIDDING_STRATEGY_COMPETITOR_DOMAIN,
Null As BIDDING_STRATEGY_CPC_BID_AMOUNT,
Null As TRACKING_URL_TEMPLATE,
Null As PARTITION_TYPE,
Null As PARENT_TYPE,
Null As USER_LIST_ELIGIBLE_FOR_SEARCH,
Null As BIDDING_STRATEGY_SOURCE,
Null As BIDDING_STRATEGY_BID_FLOOR,
Null As BIDDING_STRATEGY_MAX_CPC_BID_FLOOR,
Null As CHANNEL_NAME,
Null As CUSTOM_INTENT_ID,
Null As APP_ID,

        '{{ schema_nm }}' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM dual     

    {% endif %}
{% endfor %}