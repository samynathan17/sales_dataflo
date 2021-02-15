select 
    1 as ENTITY_ID,
    'SF_RKLIVE03022021' as ENTITY_NAME,
    'SF' as ENTITY_TYPE,
    'D_ENTITY_DIM_LOAD' as DW_SESSION_NM,
    
    current_timestamp::
    timestamp_ntz

 as DW_INS_UPD_DTS