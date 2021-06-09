








 



    
      
  select
        md5(cast(
    
    coalesce(cast(CAMPAIGN_ID as 
    varchar
), '')

 as 
    varchar
))  AS Campaign_Recpt_Act_ID,
        ACTION,
        CAMPAIGN_ID,
        MEMBER_ID,
        TIMESTAMP,
        IP,
        URL,
        LIST_ID,
        COMBINATION_ID,
        BOUNCE_TYPE,
        _FIVETRAN_SYNCED,
        'MC_ANANDLIVE_16042021' as Source_type,
        'D_CAMPAIGN_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM MC_ANANDLIVE_16042021.CAMPAIGN_RECIPIENT_ACTIVITY
         
    
