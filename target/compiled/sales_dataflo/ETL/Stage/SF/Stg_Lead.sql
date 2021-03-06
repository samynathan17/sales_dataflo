







 



   
  select
        md5(cast(
    
    coalesce(cast(id as 
    varchar
), '')

 as 
    varchar
))  AS lead_id,
        ID as Source_ID,
        IS_DELETED,
        MASTER_RECORD_ID,
        LAST_NAME,
        FIRST_NAME,
        SALUTATION,
        NAME,
        TITLE,
        COMPANY,
        STREET,
        CITY,
        STATE,
        POSTAL_CODE,
        COUNTRY,
        LATITUDE,
        LONGITUDE,
        GEOCODE_ACCURACY,
        PHONE,
        EMAIL,
        WEBSITE,
        PHOTO_URL,
        DESCRIPTION,
        LEAD_SOURCE,
        STATUS,
        INDUSTRY,
        RATING,
        ANNUAL_REVENUE,
        NUMBER_OF_EMPLOYEES,
        OWNER_ID,
        IS_CONVERTED,
        CONVERTED_DATE,
        CONVERTED_ACCOUNT_ID,
        CONVERTED_CONTACT_ID,
        CONVERTED_OPPORTUNITY_ID,
        IS_UNREAD_BY_OWNER,
        CREATED_DATE,
        CREATED_BY_ID,
        LAST_MODIFIED_DATE,
        LAST_MODIFIED_BY_ID,
        SYSTEM_MODSTAMP,
        LAST_ACTIVITY_DATE,
        LAST_VIEWED_DATE,
        LAST_REFERENCED_DATE,
        JIGSAW,
        JIGSAW_CONTACT_ID,
        EMAIL_BOUNCED_REASON,
        EMAIL_BOUNCED_DATE,
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
        'D_LEAD_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM SF_RKLIVE_06012021.Lead
          

