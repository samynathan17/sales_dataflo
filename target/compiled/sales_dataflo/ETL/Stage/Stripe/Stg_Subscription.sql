








 



    
      
  select
        md5(cast(
    
    coalesce(cast(ID as 
    varchar
), '')

 as 
    varchar
))  AS SUBSCRIPTION_ID,
        ID as SOURCE_ID,
        APPLICATION_FEE_PERCENT,
        BILLING,
        BILLING_CYCLE_ANCHOR,
        BILLING_THRESHOLD_RESET_BILLING_CYCLE_ANCHOR,
        BILLING_THRESHOLD_AMOUNT_GTE,
        CANCEL_AT,
        CANCEL_AT_PERIOD_END,
        CANCELED_AT,
        CREATED,
        CURRENT_PERIOD_END,
        CURRENT_PERIOD_START,
        DAYS_UNTIL_DUE,
        ENDED_AT,
        LIVEMODE,
        QUANTITY,
        START_DATE,
        STATUS,
        TAX_PERCENT,
        TRIAL_START,
        TRIAL_END,
        CUSTOMER_ID,
        DEFAULT_SOURCE_ID,
        METADATA,
        _FIVETRAN_SYNCED,
        'STRIPE_RKLIVE_01042021' as Entity_type,
        'D_SUBSCRIPTION_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM STRIPE_RKLIVE_01042021.SUBSCRIPTION
         
    
