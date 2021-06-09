








 



    
      
  select
        md5(cast(
    
    coalesce(cast(MEMBER_ID as 
    varchar
), '')

 as 
    varchar
))  AS Unsubscribe_ID,
        MEMBER_ID,
        CAMPAIGN_ID,
        LIST_ID,
        TIMESTAMP,
        REASON,
        _FIVETRAN_SYNCED,
        'MC_ANANDLIVE_16042021' as Source_type,
        'D_UNSUBSCRIBE_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM MC_ANANDLIVE_16042021.UNSUBSCRIBE
         
    
