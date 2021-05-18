








 



    
      
  select
        md5(cast(
    
    coalesce(cast(ID as 
    varchar
), '')

 as 
    varchar
))  AS SUBSCRIPTION_ITEM_ID,
        ID as SOURCE_ID,
        SUBSCRIPTION_ID,
        BILLING_THRESHOLDS_RESET_BILLING_CYCLE_ANCHOR,
        BILLING_THRESHOLDS_AMOUNT_GTE,
        CREATED,
        QUANTITY,
        PLAN_ID,
        METADATA,
        _FIVETRAN_SYNCED,
        'STRIPE_RKLIVE_01042021' as Entity_type,
        'D_SUBSCRIPTION_ITEM_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM STRIPE_RKLIVE_01042021.SUBSCRIPTION_ITEM
         
    
