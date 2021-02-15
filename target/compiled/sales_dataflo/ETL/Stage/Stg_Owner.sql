






     
  select
        md5(cast(
    
    coalesce(cast(OWNER_ID as 
    varchar
), '')

 as 
    varchar
))  AS OWNER_ID,
        OWNER_ID as Source_OWNER_ID,
        PORTAL_ID,
        TYPE,
        FIRST_NAME,
        LAST_NAME,
        EMAIL,
        CREATED_AT,
        UPDATED_AT,
        IS_ACTIVE,
        ACTIVE_USER_ID,
        USER_ID_INCLUDING_INACTIVE,   
        'HS_TESTUSER_09012021' as Source_type,
        'D_OWNER_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS    
    FROM HS_TESTUSER_09012021.Owner
    
    
