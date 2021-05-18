
  create or replace  view DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Creative_History  as (
    






 



  
      
  select
        md5(cast(
    
    coalesce(cast(ID as 
    varchar
), '')

 as 
    varchar
))  AS CRE_ID,
       SPONSORED_UPDATE_SHARE_CONTENT_TITLE,
SPOTLIGHT_CALL_TO_ACTION,
JOBS_LOGO,
SPONSORED_UPDATE_CAROUSEL_DIRECT_SPONSORED_CONTENT,
SPOTLIGHT_DESCRIPTION,
CALL_TO_ACTION_LABEL_TYPE,
JOBS_ORGANIZATION,
SPONSORED_VIDEO_MEDIA_ASSET,
SPONSORED_UPDATE_CAROUSEL_SHARE_CONTENT_DESCRIPTION,
SPONSORED_UPDATE_CAROUSEL_SHARE_SUBJECT,
TEXT_AD_TEXT,
FOLLOW_COMPANY_ORGANIZATION_NAME,
REVIEW_STATUS,
REFERENCE,
VERSION_TAG,
SPOTLIGHT_HEADLINE,
CAMPAIGN_ID,
SPOTLIGHT_LOGO,
SPONSORED_UPDATE_CAROUSEL_SHARE_CONTENT_SHARE_MEDIA_CATEGORY,
ID,
SPONSORED_UPDATE_SHARE,
SPONSORED_UPDATE_SHARE_SUBJECT,
SPONSORED_VIDEO_USER_GENERATED_CONTENT_POST,
SPONSORED_UPDATE_CAROUSEL_SHARE_CONTENT_TITLE,
SPOTLIGHT_CUSTOM_BACKGROUND,
CREATED_TIME,
SPONSORED_VIDEO_USER_GENERATED_CONTENT_POST_VALUE_SPECIFIC_CONTENT_SHARE_CONTENT_MEDIA,
SPONSORED_UPDATE_DIRECT_SPONSORED_CONTENT,
SPONSORED_UPDATE_CAROUSEL_SHARE_CONTENT_CONTENT_ENTITIES,
TYPE,
SPONSORED_UPDATE_ACTIVITY,
JOBS_COMPANY_NAME,
FOLLOW_COMPANY_ORGANIZATION_LOGO,
SPONSORED_UPDATE_CAROUSEL_SHARE,
SPONSORED_UPDATE_SHARE_CONTENT_SHARE_MEDIA_CATEGORY,
LAST_MODIFIED_TIME,
TEXT_AD_TITLE,
SPONSORED_UPDATE_CAROUSEL_ACTIVITY,
SPOTLIGHT_SHOW_MEMBER_PROFILE_PHOTO,
SPONSORED_IN_MAIL_CONTENT,
SPONSORED_UPDATE_SHARE_CONTENT_DESCRIPTION,
STATUS,
FOLLOW_COMPANY_CALL_TO_ACTION,
SPONSORED_UPDATE_SHARE_CONTENT_CONTENT_ENTITIES,
CALL_TO_ACTION_TARGET,
CLICK_URI,
SPOTLIGHT_FORUM_NAME,

        'LI_ADS_DATAFLO_07042021' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM LI_ADS_DATAFLO_07042021.CREATIVE_HISTORY
           
        

  );
