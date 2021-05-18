






 



  
      
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
        ACCOUNT_ID,
        IMPRESSIONS,
        INLINE_LINK_CLICKS as CLICKS,
        REACH,
        CPC,
        CPM,
        CTR,
        FREQUENCY,
        SPEND,
        AD_NAME,
        ADSET_NAME,
        'FB_ADS_DRGRILL_30032021' as Source_type,
        'D_BASIC_AD_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM FB_ADS_DRGRILL_30032021.BASIC_AD
           
        
