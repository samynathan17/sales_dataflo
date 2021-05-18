






 



  
      
  select
        md5(cast(
    
    coalesce(cast(ID as 
    varchar
), '')

 as 
    varchar
))  AS AGCH_ID,
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

        'GA_ADS_JAYANLIVE_01042021' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM GA_ADS_JAYANLIVE_01042021.AD_GROUP_CRITERION_HISTORY
           
        
