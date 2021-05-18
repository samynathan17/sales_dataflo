







 



  
 
  select
        md5(cast(
    
    coalesce(cast(id as 
    varchar
), '')

 as 
    varchar
))  AS Engagement_ID,
        ID as Source_ID,
        PORTAL_ID,
        ACTIVE,
        OWNER_ID,
        TYPE,
        ACTIVITY_TYPE,
        CREATED_AT,
        LAST_UPDATED,
        TIMESTAMP,
        _FIVETRAN_SYNCED,
        'HS_RKLIVE_01042021' as Source_type,
        'D_COMPANY_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS    
    FROM HS_RKLIVE_01042021.ENGAGEMENT
     
    
