
  create or replace  view DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Basic_Campaign  as (
    










  
         select
        NULL AS CAMPAIGN_ID,
NULL AS CTR,
NULL AS INLINE_LINK_CLICKS,
NULL AS CAMPAIGN_NAME,
NULL AS FREQUENCY,
NULL AS CPM,
NULL AS _FIVETRAN_ID,
NULL AS SPEND,
NULL AS DATE,
NULL AS ACCOUNT_ID,
NULL AS IMPRESSIONS,
NULL AS CPC,
NULL AS REACH,

        '' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM dual     

    

  );