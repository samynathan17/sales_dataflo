










  
         select
        NULL AS CAMPAIGN_NAME,
NULL AS CPM,
NULL AS ACCOUNT_ID,
NULL AS INLINE_LINK_CLICKS,
NULL AS CTR,
NULL AS SPEND,
NULL AS IMPRESSIONS,
NULL AS DATE,
NULL AS REACH,
NULL AS _FIVETRAN_ID,
NULL AS ADSET_NAME,
NULL AS CPC,
NULL AS ADSET_ID,
NULL AS FREQUENCY,

        '' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM dual     

    
