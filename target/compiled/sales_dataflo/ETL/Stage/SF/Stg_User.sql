







 



     
  select
        md5(cast(
    
    coalesce(cast(id as 
    varchar
), '')

 as 
    varchar
))  AS User_id,
        ID as Source_ID,
        USERNAME,
        LAST_NAME,
        FIRST_NAME,
        NAME,
        COMPANY_NAME,
        DIVISION,
        DEPARTMENT,
        TITLE,
        STREET,
        CITY,
        STATE,
        POSTAL_CODE,
        COUNTRY,
        EMAIL,
        EMAIL_PREFERENCES_AUTO_BCC,
        EMAIL_PREFERENCES_AUTO_BCC_STAY_IN_TOUCH,
        EMAIL_PREFERENCES_STAY_IN_TOUCH_REMINDER,
        SENDER_EMAIL,
        SENDER_NAME,
        SIGNATURE,
        STAY_IN_TOUCH_SUBJECT,
        STAY_IN_TOUCH_SIGNATURE,
        STAY_IN_TOUCH_NOTE,
        PHONE,
        FAX,
        MOBILE_PHONE,
        ALIAS,
        COMMUNITY_NICKNAME,
        BADGE_TEXT,
        IS_ACTIVE,
        TIME_ZONE_SID_KEY,
        USER_ROLE_ID,
        LOCALE_SID_KEY,
        RECEIVES_INFO_EMAILS,
        RECEIVES_ADMIN_INFO_EMAILS,
        EMAIL_ENCODING_KEY,
        USER_TYPE,
        LANGUAGE_LOCALE_KEY,
        EMPLOYEE_NUMBER,
        DELEGATED_APPROVER_ID,
        MANAGER_ID,
        LAST_LOGIN_DATE,
        CREATED_DATE,
        CREATED_BY_ID,
        LAST_MODIFIED_DATE,
        LAST_MODIFIED_BY_ID,
        contact_id,
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
        'D_USER_ROLE_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM SF_RKLIVE_06012021.user
           

