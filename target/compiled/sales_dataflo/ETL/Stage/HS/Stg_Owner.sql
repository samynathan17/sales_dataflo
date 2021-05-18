







 



     
  select
        md5(cast(
    
    coalesce(cast(OWNER_ID as 
    varchar
), '')

 as 
    varchar
))  AS OWNER_ID,
        OWNER_ID as Source_OWNER_ID,
        PORTAL_ID,
        TYPE,
        FIRST_NAME,
        LAST_NAME,
        EMAIL,
        CREATED_AT,
        UPDATED_AT,
        IS_ACTIVE,
        ACTIVE_USER_ID,
        USER_ID_INCLUDING_INACTIVE,
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
        'D_OWNER_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS    
    FROM HS_RKLIVE_01042021.Owner
        
    
