
        
        
    

    

    merge into DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Lead as DBT_INTERNAL_DEST
        using DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Lead__dbt_tmp as DBT_INTERNAL_SOURCE
        on 
            DBT_INTERNAL_SOURCE.lead_id = DBT_INTERNAL_DEST.lead_id
        

    
    when matched then update set
        "LEAD_ID" = DBT_INTERNAL_SOURCE."LEAD_ID","SOURCE_ID" = DBT_INTERNAL_SOURCE."SOURCE_ID","IS_DELETED" = DBT_INTERNAL_SOURCE."IS_DELETED","MASTER_RECORD_ID" = DBT_INTERNAL_SOURCE."MASTER_RECORD_ID","LAST_NAME" = DBT_INTERNAL_SOURCE."LAST_NAME","FIRST_NAME" = DBT_INTERNAL_SOURCE."FIRST_NAME","SALUTATION" = DBT_INTERNAL_SOURCE."SALUTATION","NAME" = DBT_INTERNAL_SOURCE."NAME","TITLE" = DBT_INTERNAL_SOURCE."TITLE","COMPANY" = DBT_INTERNAL_SOURCE."COMPANY","STREET" = DBT_INTERNAL_SOURCE."STREET","CITY" = DBT_INTERNAL_SOURCE."CITY","STATE" = DBT_INTERNAL_SOURCE."STATE","POSTAL_CODE" = DBT_INTERNAL_SOURCE."POSTAL_CODE","COUNTRY" = DBT_INTERNAL_SOURCE."COUNTRY","LATITUDE" = DBT_INTERNAL_SOURCE."LATITUDE","LONGITUDE" = DBT_INTERNAL_SOURCE."LONGITUDE","GEOCODE_ACCURACY" = DBT_INTERNAL_SOURCE."GEOCODE_ACCURACY","PHONE" = DBT_INTERNAL_SOURCE."PHONE","EMAIL" = DBT_INTERNAL_SOURCE."EMAIL","WEBSITE" = DBT_INTERNAL_SOURCE."WEBSITE","PHOTO_URL" = DBT_INTERNAL_SOURCE."PHOTO_URL","DESCRIPTION" = DBT_INTERNAL_SOURCE."DESCRIPTION","LEAD_SOURCE" = DBT_INTERNAL_SOURCE."LEAD_SOURCE","STATUS" = DBT_INTERNAL_SOURCE."STATUS","INDUSTRY" = DBT_INTERNAL_SOURCE."INDUSTRY","RATING" = DBT_INTERNAL_SOURCE."RATING","ANNUAL_REVENUE" = DBT_INTERNAL_SOURCE."ANNUAL_REVENUE","NUMBER_OF_EMPLOYEES" = DBT_INTERNAL_SOURCE."NUMBER_OF_EMPLOYEES","OWNER_ID" = DBT_INTERNAL_SOURCE."OWNER_ID","IS_CONVERTED" = DBT_INTERNAL_SOURCE."IS_CONVERTED","CONVERTED_DATE" = DBT_INTERNAL_SOURCE."CONVERTED_DATE","CONVERTED_ACCOUNT_ID" = DBT_INTERNAL_SOURCE."CONVERTED_ACCOUNT_ID","CONVERTED_CONTACT_ID" = DBT_INTERNAL_SOURCE."CONVERTED_CONTACT_ID","CONVERTED_OPPORTUNITY_ID" = DBT_INTERNAL_SOURCE."CONVERTED_OPPORTUNITY_ID","IS_UNREAD_BY_OWNER" = DBT_INTERNAL_SOURCE."IS_UNREAD_BY_OWNER","CREATED_DATE" = DBT_INTERNAL_SOURCE."CREATED_DATE","CREATED_BY_ID" = DBT_INTERNAL_SOURCE."CREATED_BY_ID","LAST_MODIFIED_DATE" = DBT_INTERNAL_SOURCE."LAST_MODIFIED_DATE","LAST_MODIFIED_BY_ID" = DBT_INTERNAL_SOURCE."LAST_MODIFIED_BY_ID","SYSTEM_MODSTAMP" = DBT_INTERNAL_SOURCE."SYSTEM_MODSTAMP","LAST_ACTIVITY_DATE" = DBT_INTERNAL_SOURCE."LAST_ACTIVITY_DATE","LAST_VIEWED_DATE" = DBT_INTERNAL_SOURCE."LAST_VIEWED_DATE","LAST_REFERENCED_DATE" = DBT_INTERNAL_SOURCE."LAST_REFERENCED_DATE","JIGSAW" = DBT_INTERNAL_SOURCE."JIGSAW","JIGSAW_CONTACT_ID" = DBT_INTERNAL_SOURCE."JIGSAW_CONTACT_ID","EMAIL_BOUNCED_REASON" = DBT_INTERNAL_SOURCE."EMAIL_BOUNCED_REASON","EMAIL_BOUNCED_DATE" = DBT_INTERNAL_SOURCE."EMAIL_BOUNCED_DATE","SOURCE_TYPE" = DBT_INTERNAL_SOURCE."SOURCE_TYPE","DW_SESSION_NM" = DBT_INTERNAL_SOURCE."DW_SESSION_NM","DW_INS_UPD_DTS" = DBT_INTERNAL_SOURCE."DW_INS_UPD_DTS"
    

    when not matched then insert
        ("LEAD_ID", "SOURCE_ID", "IS_DELETED", "MASTER_RECORD_ID", "LAST_NAME", "FIRST_NAME", "SALUTATION", "NAME", "TITLE", "COMPANY", "STREET", "CITY", "STATE", "POSTAL_CODE", "COUNTRY", "LATITUDE", "LONGITUDE", "GEOCODE_ACCURACY", "PHONE", "EMAIL", "WEBSITE", "PHOTO_URL", "DESCRIPTION", "LEAD_SOURCE", "STATUS", "INDUSTRY", "RATING", "ANNUAL_REVENUE", "NUMBER_OF_EMPLOYEES", "OWNER_ID", "IS_CONVERTED", "CONVERTED_DATE", "CONVERTED_ACCOUNT_ID", "CONVERTED_CONTACT_ID", "CONVERTED_OPPORTUNITY_ID", "IS_UNREAD_BY_OWNER", "CREATED_DATE", "CREATED_BY_ID", "LAST_MODIFIED_DATE", "LAST_MODIFIED_BY_ID", "SYSTEM_MODSTAMP", "LAST_ACTIVITY_DATE", "LAST_VIEWED_DATE", "LAST_REFERENCED_DATE", "JIGSAW", "JIGSAW_CONTACT_ID", "EMAIL_BOUNCED_REASON", "EMAIL_BOUNCED_DATE", "SOURCE_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS")
    values
        ("LEAD_ID", "SOURCE_ID", "IS_DELETED", "MASTER_RECORD_ID", "LAST_NAME", "FIRST_NAME", "SALUTATION", "NAME", "TITLE", "COMPANY", "STREET", "CITY", "STATE", "POSTAL_CODE", "COUNTRY", "LATITUDE", "LONGITUDE", "GEOCODE_ACCURACY", "PHONE", "EMAIL", "WEBSITE", "PHOTO_URL", "DESCRIPTION", "LEAD_SOURCE", "STATUS", "INDUSTRY", "RATING", "ANNUAL_REVENUE", "NUMBER_OF_EMPLOYEES", "OWNER_ID", "IS_CONVERTED", "CONVERTED_DATE", "CONVERTED_ACCOUNT_ID", "CONVERTED_CONTACT_ID", "CONVERTED_OPPORTUNITY_ID", "IS_UNREAD_BY_OWNER", "CREATED_DATE", "CREATED_BY_ID", "LAST_MODIFIED_DATE", "LAST_MODIFIED_BY_ID", "SYSTEM_MODSTAMP", "LAST_ACTIVITY_DATE", "LAST_VIEWED_DATE", "LAST_REFERENCED_DATE", "JIGSAW", "JIGSAW_CONTACT_ID", "EMAIL_BOUNCED_REASON", "EMAIL_BOUNCED_DATE", "SOURCE_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS")
