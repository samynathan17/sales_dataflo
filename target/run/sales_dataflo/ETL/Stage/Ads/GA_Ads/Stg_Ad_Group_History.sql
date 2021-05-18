
  create or replace  view DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Ad_Group_History  as (
    






 



  
      
  select
        md5(cast(
    
    coalesce(cast(ID as 
    varchar
), '')

 as 
    varchar
))  AS AGH_ID,
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

        'GA_ADS_JAYANLIVE_01042021' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM GA_ADS_JAYANLIVE_01042021.AD_GROUP_HISTORY
           
        

  );