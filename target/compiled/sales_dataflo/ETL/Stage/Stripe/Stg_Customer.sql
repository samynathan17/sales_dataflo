








 



    
      
  select
        md5(cast(
    
    coalesce(cast(ID as 
    varchar
), '')

 as 
    varchar
))  AS CUSTOMER_ID,
        ID as SOURCE_CUST_ID,
        ACCOUNT_BALANCE,
        BALANCE,
        CREATED,
        CURRENCY,
        ADDRESS_CITY,
        ADDRESS_COUNTRY,
        ADDRESS_LINE_1,
        ADDRESS_LINE_2,
        ADDRESS_POSTAL_CODE,
        ADDRESS_STATE,
        NAME,
        BANK_ACCOUNT_ID,
        SOURCE_ID,
        DEFAULT_CARD_ID,
        DELINQUENT,
        DESCRIPTION,
        EMAIL,
        PHONE,
        INVOICE_PREFIX,
        INVOICE_SETTINGS_DEFAULT_PAYMENT_METHOD,
        INVOICE_SETTINGS_FOOTER,
        LIVEMODE,
        SHIPPING_ADDRESS_CITY,
        SHIPPING_ADDRESS_COUNTRY,
        SHIPPING_ADDRESS_LINE_1,
        SHIPPING_ADDRESS_LINE_2,
        SHIPPING_ADDRESS_POSTAL_CODE,
        SHIPPING_ADDRESS_STATE,
        SHIPPING_CARRIER,
        SHIPPING_NAME,
        SHIPPING_PHONE,
        SHIPPING_TRACKING_NUMBER,
        TAX_INFO_TAX_ID,
        TAX_INFO_TYPE,
        TAX_EXEMPT,
        TAX_INFO_VERIFICATION_STATUS,
        TAX_INFO_VERIFICATION_VERIFIED_NAME,
        IS_DELETED,
        METADATA,
        _FIVETRAN_SYNCED,
        'STRIPE_RKLIVE_01042021' as Entity_type,
        'D_CUSTOMER_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM STRIPE_RKLIVE_01042021.CUSTOMER
         
    
