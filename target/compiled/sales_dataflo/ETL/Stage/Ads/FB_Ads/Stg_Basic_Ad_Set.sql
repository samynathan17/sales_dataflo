






 



  
      
  select
        md5(cast(
    
    coalesce(cast(ADSET_ID as 
    varchar
), '')

 as 
    varchar
))  AS ID,
        CAMPAIGN_NAME,
CPM,
ACCOUNT_ID,
INLINE_LINK_CLICKS,
CTR,
SPEND,
IMPRESSIONS,
DATE,
REACH,
_FIVETRAN_ID,
ADSET_NAME,
CPC,
ADSET_ID,
FREQUENCY,

        'FB_ADS_DRGRILL_30032021' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM FB_ADS_DRGRILL_30032021.BASIC_AD_SET
           
        
