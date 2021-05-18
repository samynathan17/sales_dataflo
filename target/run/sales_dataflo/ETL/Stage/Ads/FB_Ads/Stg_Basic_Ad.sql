
  create or replace  view DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Basic_Ad  as (
    










  
         select
        NULL AS AD_ID,
NULL AS CPM,
NULL AS DATE,
NULL AS ADSET_NAME,
NULL AS AD_NAME,
NULL AS SPEND,
NULL AS _FIVETRAN_ID,
NULL AS FREQUENCY,
NULL AS REACH,
NULL AS CPC,
NULL AS INLINE_LINK_CLICKS,
NULL AS CTR,
NULL AS ACCOUNT_ID,
NULL AS IMPRESSIONS,

        '' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM dual     

    

  );
