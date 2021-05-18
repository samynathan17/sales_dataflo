








 



    
      
  select
        md5(cast(
    
    coalesce(cast(BALANCE_TRANSACTION_ID as 
    varchar
), '')

 as 
    varchar
))  AS FEE_ID,
        BALANCE_TRANSACTION_ID,
        INDEX,
        CONNECTED_ACCOUNT_ID,
        AMOUNT,
        APPLICATION,
        CURRENCY,
        DESCRIPTION,
        TYPE,
        _FIVETRAN_SYNCED,
        'STRIPE_RKLIVE_01042021' as Entity_type,
        'D_FEE_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM STRIPE_RKLIVE_01042021.FEE
         
    
