

      create or replace transient table DATAFLOTEST_DATABASE.dbt_salesdataflo.stg_Page_Tracking  as
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
        PAGE_TITLE,
        LANDING_PAGE_PATH,
        PAGE_PATH,
        EXIT_PAGE_PATH,
        PAGE_VALUE,
        EXIT_RATE,
        TIME_ON_PAGE,
        PAGEVIEWS_PER_SESSION,
        UNIQUE_PAGEVIEWS,
        ENTRANCE_RATE,

        
        'GA_DATAFLO_01022021' as Source_type,
        'D_PAGE_TRACKING_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM GA_DATAFLO_01022021.PAGE_TRACKING
          
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
        PAGE_TITLE,
        LANDING_PAGE_PATH,
        PAGE_PATH,
        EXIT_PAGE_PATH,
        PAGE_VALUE,
        EXIT_RATE,
        TIME_ON_PAGE,
        PAGEVIEWS_PER_SESSION,
        UNIQUE_PAGEVIEWS,
        ENTRANCE_RATE,

        
        'GA_ANAND_01022021' as Source_type,
        'D_PAGE_TRACKING_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM GA_ANAND_01022021.PAGE_TRACKING
                  
    

      );
    