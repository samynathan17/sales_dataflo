








 



    
      
  select
        md5(cast(
    
    coalesce(cast(ID as 
    varchar
), '')

 as 
    varchar
))  AS PLAN_ID,
        ID as SOURCE_ID,
        ACTIVE,
        AGGREGATE_USAGE,
        AMOUNT,
        BILLING_SCHEME,
        CREATED,
        CURRENCY,
        INTERVAL,
        INTERVAL_COUNT,
        LIVEMODE,
        NICKNAME,
        TIERS_MODE,
        TRANSFORM_USAGE_DIVIDE_BY,
        TRANSFORM_USAGE_ROUND,
        TRIAL_PERIOD_DAYS,
        USAGE_TYPE,
        IS_DELETED,
        METADATA,
        PRODUCT_ID,
        _FIVETRAN_SYNCED,
        'STRIPE_RKLIVE_01042021' as Entity_type,
        'D_PLAN_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM STRIPE_RKLIVE_01042021.PLAN
         
    
