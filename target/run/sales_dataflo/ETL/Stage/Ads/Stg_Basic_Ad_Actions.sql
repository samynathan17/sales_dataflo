
  create or replace  view DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Basic_Ad_Actions  as (
    






 



  
      
  select
        md5(cast(
    
    coalesce(cast(AD_ID as 
    varchar
), '')

 as 
    varchar
))  AS ID,
       _1_D_VIEW,
ACTION_TYPE,
AD_ID,
INDEX,
DATE,
_FIVETRAN_ID,
_7_D_CLICK,
VALUE,

        'FB_ADS_DRGRILL_30032021' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM FB_ADS_DRGRILL_30032021.BASIC_AD_ACTIONS
           
        

  );
