
  create or replace  view DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Basic_Ad  as (
    






 



  
      
  select
        md5(cast(
    
    coalesce(cast(AD_ID as 
    varchar
), '')

 as 
    varchar
))  AS BASIC_ID,
AD_ID,
CPM,
DATE,
ADSET_NAME,
AD_NAME,
SPEND,
_FIVETRAN_ID,
FREQUENCY,
REACH,
CPC,
INLINE_LINK_CLICKS,
CTR,
ACCOUNT_ID,
IMPRESSIONS,

        'FB_ADS_DRGRILL_30032021' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM FB_ADS_DRGRILL_30032021.BASIC_AD
           
        

  );
