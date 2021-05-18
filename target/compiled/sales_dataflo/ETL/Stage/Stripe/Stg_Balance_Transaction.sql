








 



    
      
  select
        md5(cast(
    
    coalesce(cast(id as 
    varchar
), '')

 as 
    varchar
))  AS BALANCE_TRANSACTION_ID,
        ID as Source_ID,
        CONNECTED_ACCOUNT_ID,
        AMOUNT,
        AVAILABLE_ON,
        CREATED,
        CURRENCY,
        DESCRIPTION,
        EXCHANGE_RATE,
        FEE,
        NET,
        SOURCE,
        STATUS,
        TYPE,
        PAYOUT_ID,
        _FIVETRAN_SYNCED,
        'STRIPE_RKLIVE_01042021' as Entity_type,
        'D_PAYOUT_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM STRIPE_RKLIVE_01042021.BALANCE_TRANSACTION
         
    
