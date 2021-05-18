






 



  
      
  select
        md5(cast(
    
    coalesce(cast(_FIVETRAN_ID as 
    varchar
), '') || '-' || coalesce(cast(PROFILE as 
    varchar
), '') || '-' || coalesce(cast(DATE as 
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
       
        'GA_DATAFLO_22042021' as Source_type,
        'D_SOCIAL_MEDIA_ACQUISITIONS_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM GA_DATAFLO_22042021.SOCIAL_MEDIA_ACQUISITIONS
           
        
