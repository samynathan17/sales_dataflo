






 



  
      
  select
        md5(cast(
    
    coalesce(cast(ID as 
    varchar
), '')

 as 
    varchar
))  AS CAMP_ID,
        START_TIME,
        ID as CAMPAIGN_ID,
        BUDGET_REBALANCE_FLAG,
        SOURCE_CAMPAIGN_ID,
        CONFIGURED_STATUS,
        OBJECTIVE,
        STATUS,
        DAILY_BUDGET,
        BUYING_TYPE,
        NAME,
        CAN_USE_SPEND_CAP,
        EFFECTIVE_STATUS,
        BOOSTED_OBJECT_ID,
        ACCOUNT_ID,
        CREATED_TIME,
        STOP_TIME,
        CAN_CREATE_BRAND_LIFT_STUDY,
        SPEND_CAP,
        UPDATED_TIME,
        _FIVETRAN_SYNCED,
        'FB_ADS_DRGRILL_30032021' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM FB_ADS_DRGRILL_30032021.CAMPAIGN_HISTORY
           
        
