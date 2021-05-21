

      create or replace transient table DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Charge  as
      (








 



    
      
  select
        md5(cast(
    
    coalesce(cast(ID as 
    varchar
), '')

 as 
    varchar
))  AS CHARGE_ID,
        ID as SOURCE_CHARGE_ID,
        CONNECTED_ACCOUNT_ID,
        AMOUNT,
        AMOUNT_REFUNDED,
        APPLICATION,
        APPLICATION_FEE_AMOUNT,
        CALCULATED_STATEMENT_DESCRIPTOR,
        CAPTURED,
        CREATED,
        CURRENCY,
        DESCRIPTION,
        DESTINATION,
        FAILURE_CODE,
        FAILURE_MESSAGE,
        FRAUD_DETAILS_USER_REPORT,
        FRAUD_DETAILS_STRIPE_REPORT,
        LIVEMODE,
        ON_BEHALF_OF,
        OUTCOME_NETWORK_STATUS,
        OUTCOME_REASON,
        OUTCOME_RISK_LEVEL,
        OUTCOME_RISK_SCORE,
        OUTCOME_SELLER_MESSAGE,
        OUTCOME_TYPE,
        PAID,
        RECEIPT_EMAIL,
        RECEIPT_NUMBER,
        RECEIPT_URL,
        REFUNDED,
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
        CARD_ID,
        BANK_ACCOUNT_ID,
        SOURCE_ID,
        SOURCE_TRANSFER,
        STATEMENT_DESCRIPTOR,
        STATUS,
        TRANSFER_DATA_DESTINATION,
        TRANSFER_GROUP,
        BALANCE_TRANSACTION_ID,
        CUSTOMER_ID,
        INVOICE_ID,
        METADATA,
        PAYMENT_INTENT_ID,
        PAYMENT_METHOD_ID,
        TRANSFER_ID,
        _FIVETRAN_SYNCED,
        'STRIPE_RKLIVE_01042021' as Entity_type,
        'D_CHARGE_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM STRIPE_RKLIVE_01042021.CHARGE
         
    

      );
    