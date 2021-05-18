







 



    
 
  select
        md5(cast(
    
    coalesce(cast(id as 
    varchar
), '')

 as 
    varchar
))  AS Stage_id,
        ID as Source_ID,
        MASTER_LABEL,
        API_NAME,
        IS_ACTIVE,
        SORT_ORDER,
        IS_CLOSED,
        IS_WON,
        FORECAST_CATEGORY,
        FORECAST_CATEGORY_NAME,
        DEFAULT_PROBABILITY,
        DESCRIPTION,
        CREATED_BY_ID,
        CREATED_DATE,
        LAST_MODIFIED_BY_ID,
        LAST_MODIFIED_DATE,
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
        'D_OPPORTUNITYSTAGES_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM SF_RKLIVE_06012021.opportunity_stage 
             

