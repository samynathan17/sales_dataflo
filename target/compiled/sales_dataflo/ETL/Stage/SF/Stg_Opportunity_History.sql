







 



    
 
  select
        md5(cast(
    
    coalesce(cast(id as 
    varchar
), '')

 as 
    varchar
))  AS Opportunity_Calc_id,
        ID as Source_ID,        
        OPPORTUNITY_ID,
        CREATED_BY_ID,
        CREATED_DATE,
        STAGE_NAME,
        AMOUNT,
        EXPECTED_REVENUE,
        CLOSE_DATE,
        PROBABILITY,
        FORECAST_CATEGORY,
        SYSTEM_MODSTAMP,
        IS_DELETED,
        PREV_AMOUNT,
        PREV_CLOSE_DATE,
         null AS CUSTOMER_TEXT_1,
        NULL AS CUSTOMER_TEXT_2,
        NULL AS CUSTOMER_TEXT_3,
        NULL AS CUSTOMER_TEXT_4,
        NULL AS CUSTOMER_TEXT_5,
        NULL AS CUSTOMER_TEXT_6,
        NULL AS CUSTOMER_NUMBER_1,
        NULL AS CUSTOMER_NUMBER_2,
        NULL AS CUSTOMER_NUMBER_3,
        NULL AS CUSTOMER_DATE_1,
        NULL AS CUSTOMER_DATE_2,
        NULL AS CUSTOMER_DATE_3,
        'SF_RKLIVE_06012021' as Source_type,
        'D_OPPORTUNITY_HISTORY_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM SF_RKLIVE_06012021.OPPORTUNITY_HISTORY 
             

