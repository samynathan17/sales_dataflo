







 



    
  select
        md5(cast(
    
    coalesce(cast(DEAL_ID as 
    varchar
), '') || '-' || coalesce(cast(VALUE as 
    varchar
), '') || '-' || coalesce(cast(DATE_ENTERED as 
    varchar
), '')

 as 
    varchar
))  AS STAGE_ID,
        DEAL_ID as Source_deal_id,
        VALUE,
        SOURCE_ID,
        SOURCE,
        DATE_ENTERED,
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
        'HS_RKLIVE_01042021' as Source_type,
        'D_DEAL_STAGE_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS    
    FROM HS_RKLIVE_01042021.DEAL_STAGE
        
    
