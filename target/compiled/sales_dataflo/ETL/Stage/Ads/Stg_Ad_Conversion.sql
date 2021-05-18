






 



  
      
  select
        md5(cast(
    
    coalesce(cast(AD_ID as 
    varchar
), '')

 as 
    varchar
))  AS ID,
        POST_OBJECT,
OFFSITE_PIXEL,
OFFER_CREATOR,
PAGE_PARENT,
ACTION_TYPE,
PAGE,
EVENT_CREATOR,
EVENT_TYPE,
INDEX,
QUESTION,
APPLICATION,
DATASET,
OBJECT_DOMAIN,
OBJECT,
POST_OBJECT_WALL,
AD_ID,
LEADGEN,
EVENT,
QUESTION_CREATOR,
CREATIVE,
RESPONSE,
POST,
FB_PIXEL,
POST_WALL,
OFFER,
SUBTYPE,
AD_UPDATED_TIME,
FB_PIXEL_EVENT,

        'FB_ADS_DRGRILL_30032021' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM FB_ADS_DRGRILL_30032021.AD_CONVERSION
           
        
