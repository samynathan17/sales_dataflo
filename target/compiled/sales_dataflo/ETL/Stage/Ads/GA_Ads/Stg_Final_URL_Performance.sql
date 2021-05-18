






 



  
      
  select
        md5(cast(
    
    coalesce(cast(_FIVETRAN_ID as 
    varchar
), '')

 as 
    varchar
))  AS ACC_ID,
        IMPRESSIONS,
        _FIVETRAN_SYNCED,
        EFFECTIVE_FINAL_URL,
        CUSTOMER_ID,
        ACCOUNT_DESCRIPTIVE_NAME account_name,
        COST,
        CAMPAIGN_STATUS,
        CLICKS,
        AD_GROUP_STATUS,
        AD_GROUP_NAME,
        DATE,
        CAMPAIGN_NAME,
        _FIVETRAN_ID,
        CAMPAIGN_ID,
        AD_GROUP_ID,
        EXTERNAL_CUSTOMER_ID,
        'GA_ADS_JAYANLIVE_01042021' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM GA_ADS_JAYANLIVE_01042021.FINAL_URL_PERFORMANCE
           
        
