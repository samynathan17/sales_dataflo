








 



    
      
  select
        md5(cast(
    
    coalesce(cast(ID as 
    varchar
), '')

 as 
    varchar
))  AS INVOICE_LINE_ITEM_ID,
        ID as SOURCE_ID,
        INVOICE_ID,
        UNIQUE_ID,
        AMOUNT,
        CURRENCY,
        DESCRIPTION,
        DISCOUNTABLE,
        LIVEMODE,
        PERIOD_START,
        PERIOD_END,
        PRORATION,
        QUANTITY,
        TYPE,
        PLAN_ID,
        METADATA,
        SUBSCRIPTION_ID,
        SUBSCRIPTION_ITEM_ID,
        _FIVETRAN_SYNCED,
        'STRIPE_RKLIVE_01042021' as Entity_type,
        'D_INVOICE_ITEM_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM STRIPE_RKLIVE_01042021.INVOICE_LINE_ITEM
         
    
