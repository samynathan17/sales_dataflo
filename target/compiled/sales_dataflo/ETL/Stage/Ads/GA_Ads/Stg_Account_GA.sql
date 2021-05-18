






 



  
      
  select
        md5(cast(
    
    coalesce(cast(ID as 
    varchar
), '')

 as 
    varchar
))  AS ACC_ID,
        TEST_ACCOUNT,
        DATE_TIMEZONE,
        ID,
        ACCOUNT_LABEL_ID,
        ACCOUNT_LABEL_NAME,
        CURRENCY_CODE,
        _FIVETRAN_SYNCED,
        MANAGER_CUSTOMER_ID,
        SEQUENCE_ID,
        NAME,
        CAN_MANAGE_CLIENTS,
        'GA_ADS_JAYANLIVE_01042021' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM GA_ADS_JAYANLIVE_01042021.ACCOUNT
           
        
