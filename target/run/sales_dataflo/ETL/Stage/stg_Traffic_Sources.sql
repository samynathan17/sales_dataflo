

      create or replace transient table DATAFLOTEST_DATABASE.dbt_salesdataflo.stg_Traffic_Sources  as
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
        REFERRAL_PATH,
        CAMPAIGN,
        SOURCE,
        MEDIUM,
        SOURCE_MEDIUM,
        FULL_REFERRER,
        ORGANIC_SEARCHES,

        'GA_DATAFLO_01022021' as Source_type,
        'D_TRAFFIC_SOURCES_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM GA_DATAFLO_01022021.TRAFFIC_SOURCES
          
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
        REFERRAL_PATH,
        CAMPAIGN,
        SOURCE,
        MEDIUM,
        SOURCE_MEDIUM,
        FULL_REFERRER,
        ORGANIC_SEARCHES,

        'GA_ANAND_01022021' as Source_type,
        'D_TRAFFIC_SOURCES_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM GA_ANAND_01022021.TRAFFIC_SOURCES
                  
    

      );
    