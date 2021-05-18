






 



  
      
  select
        md5(cast(
    
    coalesce(cast(_FIVETRAN_ID as 
    varchar
), '')

 as 
    varchar
))  AS ID,
        AD_ID,
        DATE,
        ACTION_TYPE,
        VALUE,
        'FB_ADS_DRGRILL_30032021' as Source_type,
        'D_BASIC_CAMPAIGN_COST_PER_ACTION_TYPE_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM FB_ADS_DRGRILL_30032021.BASIC_CAMPAIGN_COST_PER_ACTION_TYPE
           
        
