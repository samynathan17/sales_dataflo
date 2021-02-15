

      create or replace transient table DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Adwords_Hourly_Stats  as
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
        DATE_HOUR,
        GOAL_VALUE_ALL,
        SESSIONS,
        GOAL_COMPLETIONS_ALL,
        PAGEVIEWS_PER_SESSION,
        GOAL_CONVERSION_RATE_ALL,
        USERS,
        BOUNCE_RATE,
        AD_CLICKS,
        AD_COST,
        CPC,    
        'GA_DATAFLO_01022021' as Source_type,
        'D_ADWORDS_HOURLY_STATS_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM GA_DATAFLO_01022021.ADWORDS_HOURLY_STATS
          
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
        DATE_HOUR,
        GOAL_VALUE_ALL,
        SESSIONS,
        GOAL_COMPLETIONS_ALL,
        PAGEVIEWS_PER_SESSION,
        GOAL_CONVERSION_RATE_ALL,
        USERS,
        BOUNCE_RATE,
        AD_CLICKS,
        AD_COST,
        CPC,    
        'GA_ANAND_01022021' as Source_type,
        'D_ADWORDS_HOURLY_STATS_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM GA_ANAND_01022021.ADWORDS_HOURLY_STATS
                  
    

      );
    