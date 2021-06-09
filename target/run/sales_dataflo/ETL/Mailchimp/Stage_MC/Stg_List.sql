

      create or replace transient table DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_List  as
      (








 



    
      
  select
        md5(cast(
    
    coalesce(cast(ID as 
    varchar
), '')

 as 
    varchar
))  AS LIST_ID,
        ID as Source_ID,
        NAME,
        CONTACT_COMPANY,
        CONTACT_ADDRESS_1,
        CONTACT_ADDRESS_2,
        CONTACT_CITY,
        CONTACT_STATE,
        CONTACT_ZIP,
        CONTACT_COUNTRY,
        VISIBILITY,
        _FIVETRAN_DELETED,
        PERMISSION_REMINDER,
        USE_ARCHIVE_BAR,
        DEFAULT_SUBJECT,
        DEFAULT_LANGUAGE,
        DEFAULT_FROM_NAME,
        DEFAULT_FROM_EMAIL,
        NOTIFY_ON_SUBSCRIBE,
        NOTIFY_ON_UNSUBSCRIBE,
        DATE_CREATED,
        LIST_RATING,
        EMAIL_TYPE_OPTION,
        SUBSCRIBE_URL_SHORT,
        SUBSCRIBE_URL_LONG,
        BEAMER_ADDRESS,
        _FIVETRAN_SYNCED,
        'MC_ANANDLIVE_16042021' as Source_type,
        'D_CAMPAIGN_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM MC_ANANDLIVE_16042021.CAMPAIGN_RECIPIENT
         
    

      );
    