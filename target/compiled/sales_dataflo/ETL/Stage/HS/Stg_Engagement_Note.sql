







 



  
 
  select
        md5(cast(
    
    coalesce(cast(ENGAGEMENT_ID as 
    varchar
), '')

 as 
    varchar
))  AS Engagement_Note_ID,
        ENGAGEMENT_ID,
        BODY,
        _FIVETRAN_SYNCED,
        'HS_RKLIVE_01042021' as Source_type,
        'D_COMPANY_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS    
    FROM HS_RKLIVE_01042021.ENGAGEMENT_NOTE
     
    
