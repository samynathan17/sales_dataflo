







 



    
 
 select
        md5(cast(
    
    coalesce(cast(id as 
    varchar
), '')

 as 
    varchar
))  AS Campaign_ID,
        ID as Source_ID,
        IS_DELETED,
        NAME,
        TYPE,
        STATUS,
        START_DATE,
        END_DATE,
        EXPECTED_REVENUE,
        BUDGETED_COST,
        ACTUAL_COST,
        EXPECTED_RESPONSE,
        NUMBER_SENT,
        IS_ACTIVE,
        DESCRIPTION,
        NUMBER_OF_LEADS,
        NUMBER_OF_CONVERTED_LEADS,
        NUMBER_OF_CONTACTS,
        NUMBER_OF_RESPONSES,
        NUMBER_OF_OPPORTUNITIES,
        NUMBER_OF_WON_OPPORTUNITIES,
        AMOUNT_ALL_OPPORTUNITIES,
        AMOUNT_WON_OPPORTUNITIES,
        OWNER_ID,
        CREATED_DATE,
        CREATED_BY_ID,
        LAST_MODIFIED_DATE,
        LAST_MODIFIED_BY_ID,
        SYSTEM_MODSTAMP,
        LAST_ACTIVITY_DATE,
        LAST_VIEWED_DATE,
        LAST_REFERENCED_DATE,
        CAMPAIGN_MEMBER_RECORD_TYPE_ID,
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
        'D_CAMPAIGN_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM SF_RKLIVE_06012021.Campaign
          
    
