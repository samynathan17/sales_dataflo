








 



    
      
  select
        md5(cast(
    
    coalesce(cast(id as 
    varchar
), '')

 as 
    varchar
))  AS PAYOUT_ID,
        ID as Source_ID,
        CONNECTED_ACCOUNT_ID,
        AMOUNT,
        ARRIVAL_DATE,
        AUTOMATIC,
        CREATED,
        CURRENCY,
        DESCRIPTION,
        FAILURE_CODE,
        FAILURE_MESSAGE,
        LIVEMODE,
        METHOD,
        SOURCE_TYPE,
        STATEMENT_DESCRIPTOR,
        STATUS,
        TYPE,
        DESTINATION_BANK_ACCOUNT_ID,
        DESTINATION_CARD_ID,
        BALANCE_TRANSACTION_ID,
        FAILURE_BALANCE_TRANSACTION_ID,
        METADATA,
        _FIVETRAN_SYNCED,
        'STRIPE_RKLIVE_01042021' as Entity_type,
        'D_PAYOUT_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM STRIPE_RKLIVE_01042021.Payout
         
    
