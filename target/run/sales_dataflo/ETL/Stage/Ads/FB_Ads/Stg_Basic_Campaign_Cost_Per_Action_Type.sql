
  create or replace  view DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Basic_Campaign_Cost_Per_Action_Type  as (
    










  
         select
       NULL AS VALUE,
NULL AS CAMPAIGN_ID,
NULL AS _1_D_VIEW,
NULL AS DATE,
NULL AS _FIVETRAN_ID,
NULL AS INDEX,
NULL AS ACTION_TYPE,
NULL AS _7_D_CLICK,

        '' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM dual     

    

  );
