
  create or replace  view DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Ad_Set_History  as (
    






 



  
      
  select
        md5(cast(
    
    coalesce(cast(ID as 
    varchar
), '')

 as 
    varchar
))  AS ID,
        PROMOTED_OBJECT_PRODUCT_SET_ID,
TARGETING_EXCLUDED_CONNECTIONS,
PROMOTED_OBJECT_OBJECT_STORE_URL,
TARGETING_USER_DEVICE,
OPTIMIZATION_GOAL,
TARGETING_EXCLUDED_PUBLISHER_CATEGORIES,
TARGETING_PUBLISHER_PLATFORMS,
BILLING_EVENT,
LIFETIME_BUDGET,
TARGETING_GEO_LOCATIONS_COUNTRIES,
PROMOTED_OBJECT_PIXEL_ID,
CREATED_TIME,
BID_STRATEGY,
TARGETING_FLEXIBLE_SPEC,
TARGETING_USER_ADCLUSTERS,
NAME,
TARGETING_WIRELESS_CARRIER,
LIFETIME_IMPS,
TARGETING_EDUCATION_STATUSES,
TARGETING_FRIENDS_OF_CONNECTIONS,
TARGETING_WORK_EMPLOYERS,
EFFECTIVE_STATUS,
BID_INFO_ACTIONS,
TARGETING_AUDIENCE_NETWORK_POSITIONS,
RF_PREDICTION_ID,
TARGETING_EDUCATION_MAJORS,
ID,
PROMOTED_OBJECT_OFFER_ID,
START_TIME,
USE_NEW_APP_CLICK,
BUDGET_REMAINING,
PROMOTED_OBJECT_EVENT_ID,
PROMOTED_OBJECT_PLACE_PAGE_SET_ID,
RECURRING_BUDGET_SEMANTICS,
UPDATED_TIME,
TARGETING_EXCLUDED_USER_DEVICE,
TARGETING_LOCALES,
TARGETING_AGE_MAX,
BID_AMOUNT,
TARGETING_EDUCATION_SCHOOLS,
TARGETING_EXCLUDED_PUBLISHER_LIST_IDS,
TARGETING_COLLEGE_YEARS,
INSTAGRAM_ACTOR_ID,
TARGETING_AGE_MIN,
TARGETING_USER_OS,
ACCOUNT_ID,
CONFIGURED_STATUS,
DAILY_BUDGET,
PROMOTED_OBJECT_APPLICATION_ID,
END_TIME,
PROMOTED_OBJECT_PAGE_ID,
CAMPAIGN_ID,
TARGETING_FACEBOOK_POSITIONS,
TARGETING_EFFECTIVE_AUDIENCE_NETWORK_POSITIONS,
TARGETING_GEO_LOCATIONS_LOCATION_TYPES,
STATUS,
ADSET_SOURCE_ID,
PROMOTED_OBJECT_CUSTOM_EVENT_TYPE,
TARGETING_CONNECTIONS,
TARGETING_WORK_POSITIONS,
TARGETING_APP_INSTALL_STATE,
DESTINATION_TYPE,
TARGETING_INSTAGRAM_POSITIONS,
TARGETING_EXCLUSIONS,
PROMOTED_OBJECT_PRODUCT_CATALOG_ID,
TARGETING_DEVICE_PLATFORMS,

        'FB_ADS_DRGRILL_30032021' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM FB_ADS_DRGRILL_30032021.AD_SET_HISTORY
           
        

  );