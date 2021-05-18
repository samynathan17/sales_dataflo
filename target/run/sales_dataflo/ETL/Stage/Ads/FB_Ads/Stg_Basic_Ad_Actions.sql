
  create or replace  view DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Basic_Ad_Actions  as (
    










  
         select
          NULL AS _1_D_VIEW,
        NULL AS ACTION_TYPE,
NULL AS AD_ID,
NULL AS INDEX,
NULL AS DATE,
NULL AS _FIVETRAN_ID,
NULL AS _7_D_CLICK,
NULL AS VALUE,

        '' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM dual     

    

  );
