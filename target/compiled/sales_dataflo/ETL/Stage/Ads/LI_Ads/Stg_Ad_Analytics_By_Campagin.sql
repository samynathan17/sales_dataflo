






 



  
      
  select
        md5(cast(
    
    coalesce(cast(CAMPAIGN_ID as 
    varchar
), '') || '-' || coalesce(cast(DAY as 
    varchar
), '')

 as 
    varchar
))  AS ID,
       LEAD_GENERATION_MAIL_CONTACT_INFO_SHARES,
VIDEO_COMPLETIONS,
VIRAL_EXTERNAL_WEBSITE_POST_CLICK_CONVERSIONS,
VIRAL_FULL_SCREEN_PLAYS,
VIRAL_SHARES,
AD_UNIT_CLICKS,
VIRAL_CARD_CLICKS,
EXTERNAL_WEBSITE_POST_CLICK_CONVERSIONS,
VIRAL_IMPRESSIONS,
EXTERNAL_WEBSITE_CONVERSIONS,
CONVERSION_VALUE_IN_LOCAL_CURRENCY,
VIRAL_VIDEO_FIRST_QUARTILE_COMPLETIONS,
CAMPAIGN_ID,
VIRAL_TOTAL_ENGAGEMENTS,
OPENS,
OTHER_ENGAGEMENTS,
VIRAL_VIDEO_VIEWS,
CARD_IMPRESSIONS,
CLICKS,
VIRAL_COMMENTS,
COMMENTS,
TEXT_URL_CLICKS,
TOTAL_ENGAGEMENTS,
VIDEO_MIDPOINT_COMPLETIONS,
VIRAL_EXTERNAL_WEBSITE_POST_VIEW_CONVERSIONS,
ACTION_CLICKS,
EXTERNAL_WEBSITE_POST_VIEW_CONVERSIONS,
VIRAL_FOLLOWS,
VIRAL_OTHER_ENGAGEMENTS,
COST_IN_USD,
DAY,
VIRAL_ONE_CLICK_LEAD_FORM_OPENS,
VIDEO_STARTS,
VIRAL_LIKES,
VIDEO_THIRD_QUARTILE_COMPLETIONS,
VIRAL_LANDING_PAGE_CLICKS,
COMMENT_LIKES,
COMPANY_PAGE_CLICKS,
IMPRESSIONS,
COST_IN_LOCAL_CURRENCY,
FOLLOWS,
LEAD_GENERATION_MAIL_INTERESTED_CLICKS,
VIRAL_CARD_IMPRESSIONS,
SHARES,
VIRAL_VIDEO_STARTS,
ONE_CLICK_LEAD_FORM_OPENS,
LANDING_PAGE_CLICKS,
VIRAL_VIDEO_COMPLETIONS,
CARD_CLICKS,
VIRAL_COMPANY_PAGE_CLICKS,
VIRAL_VIDEO_MIDPOINT_COMPLETIONS,
FULL_SCREEN_PLAYS,
VIDEO_FIRST_QUARTILE_COMPLETIONS,
VIRAL_VIDEO_THIRD_QUARTILE_COMPLETIONS,
VIDEO_VIEWS,
VIRAL_CLICKS,
VIRAL_COMMENT_LIKES,
APPROXIMATE_UNIQUE_IMPRESSIONS,
VIRAL_EXTERNAL_WEBSITE_CONVERSIONS,
VIRAL_ONE_CLICK_LEADS,
LIKES,
ONE_CLICK_LEADS,
        'LI_ADS_DATAFLO_07042021' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM LI_ADS_DATAFLO_07042021. AD_ANALYTICS_BY_CAMPAIGN
           
        
