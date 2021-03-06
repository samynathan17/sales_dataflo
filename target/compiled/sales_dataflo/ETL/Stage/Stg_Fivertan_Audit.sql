





  
      
  select
        md5(cast(
    
    coalesce(cast(ID as 
    varchar
), '')

 as 
    varchar
))  AS Audit_ID,
        ID as Source_id,
        MESSAGE,
        UPDATE_STARTED,
        UPDATE_ID,
        SCHEMA,
        --TABLE,
        --START,
        DONE,
        ROWS_UPDATED_OR_INSERTED,
        STATUS,
        PROGRESS,        
        'GA_DATAFLO_11022021' as Source_type,
        'D_FIVETRAN_AUDIT_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM GA_DATAFLO_11022021.FIVETRAN_AUDIT
                  
    
