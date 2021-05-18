
  create or replace  view DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Ad_Final_Url_History  as (
    






 



  
      
  select
        md5(cast(
    
    coalesce(cast(AD_ID as 
    varchar
), '') || '-' || coalesce(cast(AD_GROUP_ID as 
    varchar
), '')

 as 
    varchar
))  AS ID,
  _FIVETRAN_SYNCED,
AD_ID,
 AD_GROUP_ID,
SEQUENCE_ID,
URL,
        'GA_ADS_JAYANLIVE_01042021' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM GA_ADS_JAYANLIVE_01042021.AD_FINAL_URL_HISTORY
           
        

  );
