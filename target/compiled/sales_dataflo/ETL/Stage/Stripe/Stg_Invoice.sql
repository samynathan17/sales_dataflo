








 



    
      
  select
        md5(cast(
    
    coalesce(cast(ID as 
    varchar
), '')

 as 
    varchar
))  AS INVOICE_ID,
        ID as SOURCE_ID,
        AMOUNT_DUE,
        AMOUNT_PAID,
        AMOUNT_REMAINING,
        ATTEMPT_COUNT,
        ATTEMPTED,
        AUTO_ADVANCE,
        BILLING,
        BILLING_REASON,
        CURRENCY,
        CREATED,
        DATE,
        DESCRIPTION,
        DUE_DATE,
        ENDING_BALANCE,
        FINALIZED_AT,
        FOOTER,
        HOSTED_INVOICE_URL,
        INVOICE_PDF,
        LIVEMODE,
        NEXT_PAYMENT_ATTEMPT,
        NUMBER,
        PAID,
        PERIOD_START,
        PERIOD_END,
        RECEIPT_NUMBER,
        STARTING_BALANCE,
        STATEMENT_DESCRIPTOR,
        STATUS,
        SUBSCRIPTION_PRORATION_DATE,
        SUBTOTAL,
        TAX,
        TAX_PERCENT,
        THRESHOLD_REASON_AMOUNT_GTE,
        STATUS_TRANSITIONS_FINALIZED_AT,
        STATUS_TRANSITIONS_PAID_AT,
        STATUS_TRANSITIONS_VOIDED_AT,
        STATUS_TRANSITIONS_MARKED_UNCOLLECTIBLE_AT,
        TOTAL,
        WEBHOOKS_DELIVERED_AT,
        IS_DELETED,
        APPLICATION_FEE_AMOUNT,
        CHARGE_ID,
        CUSTOMER_ID,
        DEFAULT_SOURCE_ID,
        METADATA,
        _FIVETRAN_SYNCED,
        'STRIPE_RKLIVE_01042021' as Entity_type,
        'D_INVOICE_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM STRIPE_RKLIVE_01042021.INVOICE
         
    
