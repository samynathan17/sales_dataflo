






   
       
  select distinct
        md5(cast(
    
    coalesce(cast(id as 
    varchar
), '')

 as 
    varchar
))  AS Contact_ID,
        ID as Source_ID,
        IS_DELETED,
        MASTER_RECORD_ID,
        ACCOUNT_ID,
        LAST_NAME,
        FIRST_NAME,
        SALUTATION,
        NAME,
        OTHER_STREET,
        OTHER_CITY,
        OTHER_STATE,
        OTHER_POSTAL_CODE,
        OTHER_COUNTRY,
        MAILING_STREET,
        MAILING_CITY,
        MAILING_STATE,
        MAILING_POSTAL_CODE,
        MAILING_COUNTRY,
        PHONE,
        FAX,
        MOBILE_PHONE,
        HOME_PHONE,
        OTHER_PHONE,
        ASSISTANT_PHONE,
        REPORTS_TO_ID,
        EMAIL,
        TITLE,
        DEPARTMENT,
        ASSISTANT_NAME,
        LEAD_SOURCE,
        BIRTHDATE,
        DESCRIPTION,
        OWNER_ID,
        CREATED_DATE,
        CREATED_BY_ID,
        LAST_MODIFIED_DATE,
        LAST_MODIFIED_BY_ID,
        EMAIL_BOUNCED_REASON,
        EMAIL_BOUNCED_DATE,
        IS_EMAIL_BOUNCED,
        'SF_RKLIVE_06012021' as Source_type,
        'D_CONTACT_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM SF_RKLIVE_06012021.Contact
        
            UNION ALL
          
    

   
       
  select distinct
        md5(cast(
    
    coalesce(cast(id as 
    varchar
), '')

 as 
    varchar
))  AS Contact_ID,
        ID as Source_ID,
        IS_DELETED,
        MASTER_RECORD_ID,
        ACCOUNT_ID,
        LAST_NAME,
        FIRST_NAME,
        SALUTATION,
        NAME,
        OTHER_STREET,
        OTHER_CITY,
        OTHER_STATE,
        OTHER_POSTAL_CODE,
        OTHER_COUNTRY,
        MAILING_STREET,
        MAILING_CITY,
        MAILING_STATE,
        MAILING_POSTAL_CODE,
        MAILING_COUNTRY,
        PHONE,
        FAX,
        MOBILE_PHONE,
        HOME_PHONE,
        OTHER_PHONE,
        ASSISTANT_PHONE,
        REPORTS_TO_ID,
        EMAIL,
        TITLE,
        DEPARTMENT,
        ASSISTANT_NAME,
        LEAD_SOURCE,
        BIRTHDATE,
        DESCRIPTION,
        OWNER_ID,
        CREATED_DATE,
        CREATED_BY_ID,
        LAST_MODIFIED_DATE,
        LAST_MODIFIED_BY_ID,
        EMAIL_BOUNCED_REASON,
        EMAIL_BOUNCED_DATE,
        IS_EMAIL_BOUNCED,
        'SF_TESTUSER_31122020' as Source_type,
        'D_CONTACT_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM SF_TESTUSER_31122020.Contact
          
    
