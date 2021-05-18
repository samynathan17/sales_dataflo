
  create or replace  view DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Basic_Ad_Cost_Per_Action_Type  as (
    










  
         select
      NULL AS _FIVETRAN_ID,
NULL AS AD_ID,
NULL AS _7_D_CLICK,
NULL AS DATE,
NULL AS INDEX,
NULL AS _1_D_VIEW,
NULL AS VALUE,
NULL AS ACTION_TYPE,

        '' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM dual     

    

  );
