







 



  
      
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
        CONTINENT,
        COUNTRY,
        CITY,
        METRO,
        REGION,
        NETWORK_LOCATION,
        SESSIONS,
        USERS,

        
        'GA_DATAFLO_22042021' as Source_type,
        'D_GEO_NETWORK_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM GA_DATAFLO_22042021.GEO_NETWORK
                  
    
    

