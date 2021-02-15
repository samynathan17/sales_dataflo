
        
        
    

    

    merge into DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_User as DBT_INTERNAL_DEST
        using DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_User__dbt_tmp as DBT_INTERNAL_SOURCE
        on 
            DBT_INTERNAL_SOURCE.User_id = DBT_INTERNAL_DEST.User_id
        

    
    when matched then update set
        "USER_ID" = DBT_INTERNAL_SOURCE."USER_ID","SOURCE_ID" = DBT_INTERNAL_SOURCE."SOURCE_ID","USERNAME" = DBT_INTERNAL_SOURCE."USERNAME","LAST_NAME" = DBT_INTERNAL_SOURCE."LAST_NAME","FIRST_NAME" = DBT_INTERNAL_SOURCE."FIRST_NAME","NAME" = DBT_INTERNAL_SOURCE."NAME","COMPANY_NAME" = DBT_INTERNAL_SOURCE."COMPANY_NAME","DIVISION" = DBT_INTERNAL_SOURCE."DIVISION","DEPARTMENT" = DBT_INTERNAL_SOURCE."DEPARTMENT","TITLE" = DBT_INTERNAL_SOURCE."TITLE","STREET" = DBT_INTERNAL_SOURCE."STREET","CITY" = DBT_INTERNAL_SOURCE."CITY","STATE" = DBT_INTERNAL_SOURCE."STATE","POSTAL_CODE" = DBT_INTERNAL_SOURCE."POSTAL_CODE","COUNTRY" = DBT_INTERNAL_SOURCE."COUNTRY","EMAIL" = DBT_INTERNAL_SOURCE."EMAIL","EMAIL_PREFERENCES_AUTO_BCC" = DBT_INTERNAL_SOURCE."EMAIL_PREFERENCES_AUTO_BCC","EMAIL_PREFERENCES_AUTO_BCC_STAY_IN_TOUCH" = DBT_INTERNAL_SOURCE."EMAIL_PREFERENCES_AUTO_BCC_STAY_IN_TOUCH","EMAIL_PREFERENCES_STAY_IN_TOUCH_REMINDER" = DBT_INTERNAL_SOURCE."EMAIL_PREFERENCES_STAY_IN_TOUCH_REMINDER","SENDER_EMAIL" = DBT_INTERNAL_SOURCE."SENDER_EMAIL","SENDER_NAME" = DBT_INTERNAL_SOURCE."SENDER_NAME","SIGNATURE" = DBT_INTERNAL_SOURCE."SIGNATURE","STAY_IN_TOUCH_SUBJECT" = DBT_INTERNAL_SOURCE."STAY_IN_TOUCH_SUBJECT","STAY_IN_TOUCH_SIGNATURE" = DBT_INTERNAL_SOURCE."STAY_IN_TOUCH_SIGNATURE","STAY_IN_TOUCH_NOTE" = DBT_INTERNAL_SOURCE."STAY_IN_TOUCH_NOTE","PHONE" = DBT_INTERNAL_SOURCE."PHONE","FAX" = DBT_INTERNAL_SOURCE."FAX","MOBILE_PHONE" = DBT_INTERNAL_SOURCE."MOBILE_PHONE","ALIAS" = DBT_INTERNAL_SOURCE."ALIAS","COMMUNITY_NICKNAME" = DBT_INTERNAL_SOURCE."COMMUNITY_NICKNAME","BADGE_TEXT" = DBT_INTERNAL_SOURCE."BADGE_TEXT","IS_ACTIVE" = DBT_INTERNAL_SOURCE."IS_ACTIVE","TIME_ZONE_SID_KEY" = DBT_INTERNAL_SOURCE."TIME_ZONE_SID_KEY","USER_ROLE_ID" = DBT_INTERNAL_SOURCE."USER_ROLE_ID","LOCALE_SID_KEY" = DBT_INTERNAL_SOURCE."LOCALE_SID_KEY","RECEIVES_INFO_EMAILS" = DBT_INTERNAL_SOURCE."RECEIVES_INFO_EMAILS","RECEIVES_ADMIN_INFO_EMAILS" = DBT_INTERNAL_SOURCE."RECEIVES_ADMIN_INFO_EMAILS","EMAIL_ENCODING_KEY" = DBT_INTERNAL_SOURCE."EMAIL_ENCODING_KEY","USER_TYPE" = DBT_INTERNAL_SOURCE."USER_TYPE","LANGUAGE_LOCALE_KEY" = DBT_INTERNAL_SOURCE."LANGUAGE_LOCALE_KEY","EMPLOYEE_NUMBER" = DBT_INTERNAL_SOURCE."EMPLOYEE_NUMBER","DELEGATED_APPROVER_ID" = DBT_INTERNAL_SOURCE."DELEGATED_APPROVER_ID","MANAGER_ID" = DBT_INTERNAL_SOURCE."MANAGER_ID","LAST_LOGIN_DATE" = DBT_INTERNAL_SOURCE."LAST_LOGIN_DATE","CREATED_DATE" = DBT_INTERNAL_SOURCE."CREATED_DATE","CREATED_BY_ID" = DBT_INTERNAL_SOURCE."CREATED_BY_ID","LAST_MODIFIED_DATE" = DBT_INTERNAL_SOURCE."LAST_MODIFIED_DATE","LAST_MODIFIED_BY_ID" = DBT_INTERNAL_SOURCE."LAST_MODIFIED_BY_ID","CONTACT_ID" = DBT_INTERNAL_SOURCE."CONTACT_ID","SOURCE_TYPE" = DBT_INTERNAL_SOURCE."SOURCE_TYPE","DW_SESSION_NM" = DBT_INTERNAL_SOURCE."DW_SESSION_NM","DW_INS_UPD_DTS" = DBT_INTERNAL_SOURCE."DW_INS_UPD_DTS"
    

    when not matched then insert
        ("USER_ID", "SOURCE_ID", "USERNAME", "LAST_NAME", "FIRST_NAME", "NAME", "COMPANY_NAME", "DIVISION", "DEPARTMENT", "TITLE", "STREET", "CITY", "STATE", "POSTAL_CODE", "COUNTRY", "EMAIL", "EMAIL_PREFERENCES_AUTO_BCC", "EMAIL_PREFERENCES_AUTO_BCC_STAY_IN_TOUCH", "EMAIL_PREFERENCES_STAY_IN_TOUCH_REMINDER", "SENDER_EMAIL", "SENDER_NAME", "SIGNATURE", "STAY_IN_TOUCH_SUBJECT", "STAY_IN_TOUCH_SIGNATURE", "STAY_IN_TOUCH_NOTE", "PHONE", "FAX", "MOBILE_PHONE", "ALIAS", "COMMUNITY_NICKNAME", "BADGE_TEXT", "IS_ACTIVE", "TIME_ZONE_SID_KEY", "USER_ROLE_ID", "LOCALE_SID_KEY", "RECEIVES_INFO_EMAILS", "RECEIVES_ADMIN_INFO_EMAILS", "EMAIL_ENCODING_KEY", "USER_TYPE", "LANGUAGE_LOCALE_KEY", "EMPLOYEE_NUMBER", "DELEGATED_APPROVER_ID", "MANAGER_ID", "LAST_LOGIN_DATE", "CREATED_DATE", "CREATED_BY_ID", "LAST_MODIFIED_DATE", "LAST_MODIFIED_BY_ID", "CONTACT_ID", "SOURCE_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS")
    values
        ("USER_ID", "SOURCE_ID", "USERNAME", "LAST_NAME", "FIRST_NAME", "NAME", "COMPANY_NAME", "DIVISION", "DEPARTMENT", "TITLE", "STREET", "CITY", "STATE", "POSTAL_CODE", "COUNTRY", "EMAIL", "EMAIL_PREFERENCES_AUTO_BCC", "EMAIL_PREFERENCES_AUTO_BCC_STAY_IN_TOUCH", "EMAIL_PREFERENCES_STAY_IN_TOUCH_REMINDER", "SENDER_EMAIL", "SENDER_NAME", "SIGNATURE", "STAY_IN_TOUCH_SUBJECT", "STAY_IN_TOUCH_SIGNATURE", "STAY_IN_TOUCH_NOTE", "PHONE", "FAX", "MOBILE_PHONE", "ALIAS", "COMMUNITY_NICKNAME", "BADGE_TEXT", "IS_ACTIVE", "TIME_ZONE_SID_KEY", "USER_ROLE_ID", "LOCALE_SID_KEY", "RECEIVES_INFO_EMAILS", "RECEIVES_ADMIN_INFO_EMAILS", "EMAIL_ENCODING_KEY", "USER_TYPE", "LANGUAGE_LOCALE_KEY", "EMPLOYEE_NUMBER", "DELEGATED_APPROVER_ID", "MANAGER_ID", "LAST_LOGIN_DATE", "CREATED_DATE", "CREATED_BY_ID", "LAST_MODIFIED_DATE", "LAST_MODIFIED_BY_ID", "CONTACT_ID", "SOURCE_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS")
