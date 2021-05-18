








 



    
      
  select
        md5(cast(
    
    coalesce(cast(ID as 
    varchar
), '')

 as 
    varchar
))  AS PAYMENT_INTENT_ID,
        ID as SOURCE_PAYMENT_ID,
        AMOUNT,
        AMOUNT_CAPTURABLE,
        AMOUNT_RECEIVED,
        APPLICATION,
        APPLICATION_FEE_AMOUNT,
        CANCELED_AT,
        CANCELLATION_REASON,
        CAPTURE_METHOD,
        CONFIRMATION_METHOD,
        CREATED,
        CURRENCY,
        DESCRIPTION,
        LAST_PAYMENT_ERROR_TYPE,
        LAST_PAYMENT_ERROR_CODE,
        LAST_PAYMENT_ERROR_DECLINE_CODE,
        LAST_PAYMENT_ERROR_DOC_URL,
        LAST_PAYMENT_ERROR_MESSAGE,
        LAST_PAYMENT_ERROR_PARAM,
        LAST_PAYMENT_ERROR_SOURCE_ID,
        LAST_PAYMENT_ERROR_CHARGE_ID,
        LIVEMODE,
        ON_BEHALF_OF,
        RECEIPT_EMAIL,
        STATEMENT_DESCRIPTOR,
        STATUS,
        TRANSFER_DATA_DESTINATION,
        TRANSFER_GROUP,
        PAYMENT_METHOD_ID,
        CUSTOMER_ID,
        METADATA,
        SOURCE_ID,
        _FIVETRAN_SYNCED,
        'STRIPE_RKLIVE_01042021' as Entity_type,
        'D_PAYMENT_INTENT_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM STRIPE_RKLIVE_01042021.PAYMENT_INTENT
         
    
