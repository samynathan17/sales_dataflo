
  create or replace  view DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Ad_History  as (
    






 



  
      
  select
        md5(cast(
    
    coalesce(cast(ID as 
    varchar
), '')

 as 
    varchar
))  AS AD_HIS_ID,
       CONFIGURED_STATUS,
BID_INFO_ACTIONS,
ACCOUNT_ID,
STATUS,
LAST_UPDATED_BY_APP_ID,
CREATED_TIME,
EFFECTIVE_STATUS,
CREATIVE_ID,
NAME,
UPDATED_TIME,
AD_SET_ID,
BID_AMOUNT,
AD_SOURCE_ID,
BID_TYPE,
ID,
CAMPAIGN_ID,
_FIVETRAN_SYNCED,
        'FB_ADS_DRGRILL_30032021' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM FB_ADS_DRGRILL_30032021.AD_HISTORY
           
        

  );
