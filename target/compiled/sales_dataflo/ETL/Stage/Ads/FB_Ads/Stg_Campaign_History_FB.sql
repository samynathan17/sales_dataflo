










  
         select
        Null as ID,
        Null as START_TIME,
        Null as CAMPAIGN_ID,
        Null as BUDGET_REBALANCE_FLAG,
        Null as SOURCE_CAMPAIGN_ID,
        Null as CONFIGURED_STATUS,
        Null as OBJECTIVE,
        Null as STATUS,
        Null as DAILY_BUDGET,
        Null as BUYING_TYPE,
        Null as NAME,
        Null as CAN_USE_SPEND_CAP,
        Null as EFFECTIVE_STATUS,
        Null as BOOSTED_OBJECT_ID,
        Null as ACCOUNT_ID,
        Null as CREATED_TIME,
        Null as STOP_TIME,
        Null as CAN_CREATE_BRAND_LIFT_STUDY,
        Null as SPEND_CAP,
        Null as UPDATED_TIME,
        '' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM dual     

    
