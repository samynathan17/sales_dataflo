








 



    
      
  select
        md5(cast(
    
    coalesce(cast(ID as 
    varchar
), '')

 as 
    varchar
))  AS INVOICE_ITEM_ID,
        ID as SOURCE_ID,
        AMOUNT,
        CURRENCY,
        DATE,
        DESCRIPTION,
        DISCOUNTABLE,
        LIVEMODE,
        PERIOD_START,
        PERIOD_END,
        PRORATION,
        QUANTITY,
        UNIT_AMOUNT,
        IS_DELETED,
        CUSTOMER_ID,
        INVOICE_ID,
        METADATA,
        SUBSCRIPTION_ID,
        SUBSCRIPTION_ITEM_ID,
        _FIVETRAN_SYNCED,
        'STRIPE_RKLIVE_01042021' as Entity_type,
        'D_INVOICE_ITEM_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM STRIPE_RKLIVE_01042021.INVOICE_ITEM
         
    
