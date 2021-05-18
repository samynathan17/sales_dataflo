








 



    
      
  select
        md5(cast(
    
    coalesce(cast(id as 
    varchar
), '')

 as 
    varchar
))  AS Account_ID,
        ID as Source_ID,
        IS_DELETED,
        MASTER_RECORD_ID,
        NAME,
        TYPE,
        PARENT_ID,
        BILLING_STREET,
        BILLING_CITY,
        BILLING_STATE,
        BILLING_POSTAL_CODE,
        BILLING_COUNTRY,
        SHIPPING_STREET,
        SHIPPING_CITY,
        SHIPPING_STATE,
        SHIPPING_POSTAL_CODE,
        SHIPPING_COUNTRY,
        PHONE,
        FAX,
        WEBSITE,
        SIC,
        INDUSTRY,
        ANNUAL_REVENUE,
        NUMBER_OF_EMPLOYEES,
        OWNERSHIP,
        DESCRIPTION,
        OWNER_ID,
        CREATED_DATE,
        CREATED_BY_ID,
        LAST_MODIFIED_DATE,
        LAST_MODIFIED_BY_ID,
        ACCOUNT_SOURCE,
        SIC_DESC,
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
        'D_ACCOUNT_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM SF_RKLIVE_06012021.Account
         
    
