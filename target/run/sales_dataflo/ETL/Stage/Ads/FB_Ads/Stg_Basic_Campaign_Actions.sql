
  create or replace  view DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Basic_Campaign_Actions  as (
    






 



  
      
  select
        md5(cast(
    
    coalesce(cast(_FIVETRAN_ID as 
    varchar
), '')

 as 
    varchar
))  AS ID,
        _7_D_CLICK,
_1_D_VIEW,
CAMPAIGN_ID,
ACTION_TYPE,
INDEX,
VALUE,
DATE,
_FIVETRAN_ID,

        'FB_ADS_DRGRILL_30032021' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM FB_ADS_DRGRILL_30032021.BASIC_CAMPAIGN_ACTIONS
           
        

  );
