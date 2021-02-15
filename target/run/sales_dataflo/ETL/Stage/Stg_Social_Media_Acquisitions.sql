

      create or replace transient table DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Social_Media_Acquisitions  as
      (






  
      
  select
        md5(cast(
    
    coalesce(cast(_FIVETRAN_ID as 
    varchar
), '')

 as 
    varchar
))  AS ID,
        DATE,
        PROFILE,
        SOCIAL_NETWORK,
        SESSIONS,
        NEW_USERS,
        AVG_SESSION_DURATION,
        TRANSACTION_REVENUE,
        PAGEVIEWS_PER_SESSION,
        TRANSACTIONS,
        BOUNCE_RATE,
        PAGEVIEWS,
        PERCENT_NEW_SESSIONS,
        TRANSACTIONS_PER_SESSION,
       
        'GA_DATAFLO_01022021' as Source_type,
        'D_SOCIAL_MEDIA_ACQUISITIONS_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM GA_DATAFLO_01022021.SOCIAL_MEDIA_ACQUISITIONS
          
            UNION ALL
                
    


  
      
  select
        md5(cast(
    
    coalesce(cast(_FIVETRAN_ID as 
    varchar
), '')

 as 
    varchar
))  AS ID,
        DATE,
        PROFILE,
        SOCIAL_NETWORK,
        SESSIONS,
        NEW_USERS,
        AVG_SESSION_DURATION,
        TRANSACTION_REVENUE,
        PAGEVIEWS_PER_SESSION,
        TRANSACTIONS,
        BOUNCE_RATE,
        PAGEVIEWS,
        PERCENT_NEW_SESSIONS,
        TRANSACTIONS_PER_SESSION,
       
        'GA_ANAND_01022021' as Source_type,
        'D_SOCIAL_MEDIA_ACQUISITIONS_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM GA_ANAND_01022021.SOCIAL_MEDIA_ACQUISITIONS
                  
    

      );
    