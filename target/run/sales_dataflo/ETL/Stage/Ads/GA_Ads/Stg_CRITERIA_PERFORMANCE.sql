
  create or replace  view DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_CRITERIA_PERFORMANCE  as (
    






 



  
      
  select
        md5(cast(
    
    coalesce(cast(ID as 
    varchar
), '')

 as 
    varchar
))  AS ACC_ID,
       CAMPAIGN_STATUS,
_FIVETRAN_SYNCED,
ACCOUNT_DESCRIPTIVE_NAME,
_FIVETRAN_ID,
AD_GROUP_STATUS,
AD_GROUP_NAME,
DATE,
CAMPAIGN_ID,
EXTERNAL_CUSTOMER_ID,
CRITERIA,
AD_GROUP_ID,
CUSTOMER_ID,
ID,
CRITERIA_TYPE,
COST,
CAMPAIGN_NAME,
IMPRESSIONS,
CLICKS,
CRITERIA_DESTINATION_URL,

        'GA_ADS_JAYANLIVE_01042021' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM GA_ADS_JAYANLIVE_01042021.CRITERIA_PERFORMANCE
           
        

  );
