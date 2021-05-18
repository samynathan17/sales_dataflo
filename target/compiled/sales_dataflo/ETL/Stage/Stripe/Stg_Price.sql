








 



    
      
  select
        md5(cast(
    
    coalesce(cast(ID as 
    varchar
), '')

 as 
    varchar
))  AS PRICE_ID,
        ID as SOURCE_ID,
        ACTIVE,
        CURRENCY,
        NICKNAME,
        RECURRING_AGGREGATE_USAGE,
        RECURRING_INTERVAL,
        RECURRING_INTERVAL_COUNT,
        RECURRING_USAGE_TYPE,
        TYPE,
        UNIT_AMOUNT,
        BILLING_SCHEME,
        CREATED,
        LIVEMODE,
        LOOKUP_KEY,
        TIERS_MODE,
        TRANSFORM_QUANTITY_DIVIDE_BY,
        TRANSFORM_QUANTITY_ROUND,
        UNIT_AMOUNT_DECIMAL,
        IS_DELETED,
        PRODUCT_ID,
        _FIVETRAN_SYNCED,
        'STRIPE_RKLIVE_01042021' as Entity_type,
        'D_PRICE_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM STRIPE_RKLIVE_01042021.PRICE
         
    
