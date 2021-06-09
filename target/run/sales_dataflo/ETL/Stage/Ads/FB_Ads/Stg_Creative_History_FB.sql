
  create or replace  view DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Creative_History_FB  as (
    






 



  
      
  select
        md5(cast(
    
    coalesce(cast(ID as 
    varchar
), '')

 as 
    varchar
))  AS CRE_ID,
        OBJECT_STORY_LINK_DATA_APP_LINK_SPEC_IOS,
ID as CREATIVE_ID,
OBJECT_STORY_LINK_DATA_APP_LINK_SPEC_IPAD,
OBJECT_STORY_LINK_DATA_DESCRIPTION,
TEMPLATE_APP_LINK_SPEC_IOS,
TITLE,
TEMPLATE_LINK,
VIDEO_CALL_TO_ACTION_VALUE_LINK,
ACTOR_ID,
OBJECT_STORY_LINK_DATA_CAPTION,
OBJECT_URL,
_FIVETRAN_ID,
EFFECTIVE_INSTAGRAM_STORY_ID,
PAGE_MESSAGE,
CAROUSEL_AD_LINK,
ASSET_FEED_SPEC_LINK_URLS,
OBJECT_STORY_ID,
CALL_TO_ACTION_TYPE,
USE_PAGE_ACTOR_OVERRIDE,
IMAGE_URL,
INSTAGRAM_PERMALINK_URL,
NAME as CREATIVE_NAME,
TEMPLATE_CHILD_ATTACHMENTS,
PAGE_LINK,
TEMPLATE_MESSAGE,
TEMPLATE_APP_LINK_SPEC_IPHONE,
TEMPLATE_PAGE_LINK,
OBJECT_STORY_LINK_DATA_LINK,
OBJECT_ID,
TEMPLATE_APP_LINK_SPEC_ANDROID,
OBJECT_STORY_LINK_DATA_APP_LINK_SPEC_ANDROID,
EFFECTIVE_OBJECT_STORY_ID,
PRODUCT_SET_ID,
IMAGE_FILE,
BODY,
OBJECT_STORY_LINK_DATA_APP_LINK_SPEC_IPHONE,
STATUS,
OBJECT_STORY_LINK_DATA_MESSAGE,
THUMBNAIL_URL,
TEMPLATE_DESCRIPTION,
VIDEO_ID,
INSTAGRAM_STORY_ID,
OBJECT_TYPE,
LINK_OG_ID,
BRANDED_CONTENT_SPONSOR_PAGE_ID,
TEMPLATE_CAPTION,
TEMPLATE_URL,
ACCOUNT_ID,
IMAGE_HASH,
LINK_URL,
APPLINK_TREATMENT,
TEMPLATE_APP_LINK_SPEC_IPAD,
INSTAGRAM_ACTOR_ID,
OBJECT_STORY_LINK_DATA_CHILD_ATTACHMENTS,
URL_TAGS,
_FIVETRAN_SYNCED,
        'FB_ADS_DRGRILL_30032021' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM FB_ADS_DRGRILL_30032021.CREATIVE_HISTORY
           
        

  );
