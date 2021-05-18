
  create or replace  view DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Basic_Campaign_Cost_Per_Action_Type  as (
    






 



  
      
  select
        md5(cast(
    
    coalesce(cast(CAMPAIGN_ID as 
    varchar
), '')

 as 
    varchar
))  AS ID,
       CAMPAIGN_ID,
 _1_D_VIEW,
 DATE,
 _FIVETRAN_ID,
 INDEX,
 ACTION_TYPE,
 _7_D_CLICK,

        'FB_ADS_DRGRILL_30032021' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM FB_ADS_DRGRILL_30032021.BASIC_CAMPAIGN_COST_PER_ACTION_TYPE
           
        

  );
