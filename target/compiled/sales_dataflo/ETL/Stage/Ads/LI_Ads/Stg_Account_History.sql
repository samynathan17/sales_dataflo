





 



  
      
  select
        md5(cast(
    
    coalesce(cast(ID as 
    varchar
), '')

 as 
    varchar
))  AS ACC_ID,
NOTIFIED_ON_CREATIVE_REJECTION,
TYPE,
NOTIFIED_ON_CREATIVE_APPROVAL,
ID,
LAST_MODIFIED_TIME,
NAME,
CREATED_TIME,
NOTIFIED_ON_CAMPAIGN_OPTIMIZATION,
NOTIFIED_ON_END_OF_CAMPAIGN,
STATUS,
REFERENCE,
CURRENCY,
VERSION_TAG,

        'LI_ADS_DATAFLO_07042021' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM LI_ADS_DATAFLO_07042021.ACCOUNT_HISTORY
           
        
