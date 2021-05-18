






 



  
      
  select
        md5(cast(
    
    coalesce(cast(CREATIVE_ID as 
    varchar
), '')

 as 
    varchar
))  AS ID,
SHARES,
COMPANY_PAGE_CLICKS,
VIRAL_SHARES,
VIRAL_VIDEO_FIRST_QUARTILE_COMPLETIONS,
OTHER_ENGAGEMENTS,
VIRAL_CARD_CLICKS,
VIDEO_VIEWS,
VIRAL_VIDEO_MIDPOINT_COMPLETIONS,
VIRAL_EXTERNAL_WEBSITE_POST_CLICK_CONVERSIONS,
VIRAL_ONE_CLICK_LEADS,
CARD_CLICKS,
VIDEO_MIDPOINT_COMPLETIONS,
VIDEO_COMPLETIONS,
VIRAL_IMPRESSIONS,
VIRAL_COMMENT_LIKES,
VIRAL_LANDING_PAGE_CLICKS,
FULL_SCREEN_PLAYS,
VIDEO_STARTS,
VIRAL_COMMENTS,
LIKES,
VIRAL_OTHER_ENGAGEMENTS,
DAY,
VIRAL_FULL_SCREEN_PLAYS,
APPROXIMATE_UNIQUE_IMPRESSIONS,
COMMENT_LIKES,
EXTERNAL_WEBSITE_POST_CLICK_CONVERSIONS,
COST_IN_LOCAL_CURRENCY,
ONE_CLICK_LEADS,
VIRAL_EXTERNAL_WEBSITE_CONVERSIONS,
LEAD_GENERATION_MAIL_INTERESTED_CLICKS,
OPENS,
VIDEO_FIRST_QUARTILE_COMPLETIONS,
VIRAL_CLICKS,
EXTERNAL_WEBSITE_CONVERSIONS,
VIRAL_CARD_IMPRESSIONS,
ONE_CLICK_LEAD_FORM_OPENS,
VIRAL_EXTERNAL_WEBSITE_POST_VIEW_CONVERSIONS,
CREATIVE_ID,
EXTERNAL_WEBSITE_POST_VIEW_CONVERSIONS,
VIRAL_VIDEO_THIRD_QUARTILE_COMPLETIONS,
VIRAL_VIDEO_VIEWS,
LANDING_PAGE_CLICKS,
ACTION_CLICKS,
CONVERSION_VALUE_IN_LOCAL_CURRENCY,
COST_IN_USD,
VIRAL_ONE_CLICK_LEAD_FORM_OPENS,
VIRAL_TOTAL_ENGAGEMENTS,
VIRAL_VIDEO_STARTS,
CLICKS,
LEAD_GENERATION_MAIL_CONTACT_INFO_SHARES,
TEXT_URL_CLICKS,
VIRAL_LIKES,
IMPRESSIONS,
AD_UNIT_CLICKS,
CARD_IMPRESSIONS,
VIRAL_FOLLOWS,
FOLLOWS,
TOTAL_ENGAGEMENTS,
VIDEO_THIRD_QUARTILE_COMPLETIONS,
COMMENTS,
VIRAL_COMPANY_PAGE_CLICKS,
VIRAL_VIDEO_COMPLETIONS,

        'LINKEDIN_ADS_19032021' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM LINKEDIN_ADS_19032021.AD_ANALYTICS_BY_CREATIVE
           
        