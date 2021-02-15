

      create or replace transient table DATAFLOTEST_DATABASE.dbt_salesdataflo.stg_Geo_Network  as
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
        CONTINENT,
        COUNTRY,
        CITY,
        METRO,
        REGION,
        NETWORK_LOCATION,
        SESSIONS,
        USERS,

        
        'GA_DATAFLO_01022021' as Source_type,
        'D_GEO_NETWORK_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM GA_DATAFLO_01022021.GEO_NETWORK
          
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
        CONTINENT,
        COUNTRY,
        CITY,
        METRO,
        REGION,
        NETWORK_LOCATION,
        SESSIONS,
        USERS,

        
        'GA_ANAND_01022021' as Source_type,
        'D_GEO_NETWORK_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM GA_ANAND_01022021.GEO_NETWORK
                  
    

      );
    