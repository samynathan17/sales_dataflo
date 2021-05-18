
  create or replace  view DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_CLICK_PERFORMANCE  as (
    






 



  
      
  select
        md5(cast(
    
    coalesce(cast(_FIVETRAN_ID as 
    varchar
), '')

 as 
    varchar
))  AS ACC_ID,
       AD_GROUP_ID,

CLICKS,
EXTERNAL_CUSTOMER_ID,
AD_GROUP_STATUS,
CAMPAIGN_STATUS,
ACCOUNT_DESCRIPTIVE_NAME,
GCL_ID,
AD_GROUP_NAME,
CAMPAIGN_ID,
_FIVETRAN_ID,
CRITERIA_ID,
CAMPAIGN_NAME,
DATE,

        'GA_ADS_JAYANLIVE_01042021' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM GA_ADS_JAYANLIVE_01042021.CLICK_PERFORMANCE
           
        

  );
