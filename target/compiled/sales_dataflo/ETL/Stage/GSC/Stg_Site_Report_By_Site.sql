



 



  
      
  select
        md5(cast(
    
    coalesce(cast(COUNTRY as 
    varchar
), '') || '-' || coalesce(cast(DATE as 
    varchar
), '') || '-' || coalesce(cast(DEVICE as 
    varchar
), '')

 as 
    varchar
))  AS Site_Rept_ID,
        COUNTRY,
        DATE,
        DEVICE,
        SEARCH_TYPE,
        SITE,
        CLICKS,
        IMPRESSIONS,
        CTR,
        POSITION,
        _FIVETRAN_SYNCED,
        'GSC_ANANDLIVE_14052021' as Source_type,
        'D_SITE_REPORT_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM GSC_ANANDLIVE_14052021.SITE_REPORT_BY_SITE
         
        
