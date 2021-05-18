
  create or replace  view DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Account_History  as (
    






 



  
      
  select
        md5(cast(
    
    coalesce(cast(ID as 
    varchar
), '')

 as 
    varchar
))  AS ID,
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

        'LINKEDIN_ADS_19032021' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM LINKEDIN_ADS_19032021.ACCOUNT_HISTORY
           
        

  );
