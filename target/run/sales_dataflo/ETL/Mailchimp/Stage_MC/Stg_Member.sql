

      create or replace transient table DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Member  as
      (








 



    
      
  select
        md5(cast(
    
    coalesce(cast(CAMPAIGN_ID as 
    varchar
), '')

 as 
    varchar
))  AS MEMBER_ID,
        ID as Source_ID,
        LIST_ID,
        STATUS,
        LANGUAGE,
        VIP,
        LATITUDE,
        LONGITUDE,
        GMTOFF,
        DSTOFF,
        COUNTRY_CODE,
        TIMEZONE,
        SOURCE,
        EMAIL_ADDRESS,
        UNIQUE_EMAIL_ID,
        EMAIL_TYPE,
        IP_SIGNUP,
        TIMESTAMP_SIGNUP,
        IP_OPT,
        TIMESTAMP_OPT,
        MEMBER_RATING,
        LAST_CHANGED,
        EMAIL_CLIENT,
        UNSUBSCRIBE_REASON,
        MERGE_LNAME,
        MERGE_FNAME,
        _FIVETRAN_SYNCED,
        'MC_ANANDLIVE_16042021' as Source_type,
        'D_MEMBER_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM MC_ANANDLIVE_16042021.MEMBER
         
    

      );
    