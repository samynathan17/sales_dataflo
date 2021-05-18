
  create or replace  view DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_BASIC_AD_ACTIONS  as (
    






 



  
      
  select
        md5(cast(
    
    coalesce(cast(_FIVETRAN_ID as 
    varchar
), '')

 as 
    varchar
))  AS ID,
        AD_ID,
        DATE,
        ACTION_TYPE,
        VALUE,
        'FB_ADS_DRGRILL_30032021' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM FB_ADS_DRGRILL_30032021.BASIC_AD_ACTIONS
           
        

  );