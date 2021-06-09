






 



  
      
  select
        md5(cast(
    
    coalesce(cast(_FIVETRAN_ID as 
    varchar
), '')

 as 
    varchar
))  AS ID,
        CAMPAIGN_ID,
CTR,
INLINE_LINK_CLICKS,
CAMPAIGN_NAME,
FREQUENCY,
CPM,
_FIVETRAN_ID,
SPEND,
DATE,
ACCOUNT_ID,
IMPRESSIONS,
CPC,
REACH,

        'FB_ADS_DRGRILL_30032021' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM FB_ADS_DRGRILL_30032021.BASIC_CAMPAIGN
           
        
