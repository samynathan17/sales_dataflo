with base as (

    select *
    from DATAFLOTEST_DATABASE.dbt_salesdataflo.stg_linkedin__ad_analytics_by_creative_tmp

), macro as (

    select
        
    
    
    _fivetran_synced
    
 as 
    
    _fivetran_synced
    
, 
    
    
    action_clicks
    
 as 
    
    action_clicks
    
, 
    
    
    ad_unit_clicks
    
 as 
    
    ad_unit_clicks
    
, 
    
    
    approximate_unique_impressions
    
 as 
    
    approximate_unique_impressions
    
, 
    
    
    card_clicks
    
 as 
    
    card_clicks
    
, 
    
    
    card_impressions
    
 as 
    
    card_impressions
    
, 
    
    
    clicks
    
 as 
    
    clicks
    
, 
    
    
    comment_likes
    
 as 
    
    comment_likes
    
, 
    
    
    comments
    
 as 
    
    comments
    
, 
    cast(null as 
    int
) as 
    
    comments_likes
    
 , 
    
    
    company_page_clicks
    
 as 
    
    company_page_clicks
    
, 
    
    
    conversion_value_in_local_currency
    
 as 
    
    conversion_value_in_local_currency
    
, 
    
    
    cost_in_local_currency
    
 as 
    
    cost_in_local_currency
    
, 
    
    
    cost_in_usd
    
 as 
    
    cost_in_usd
    
, 
    
    
    creative_id
    
 as 
    
    creative_id
    
, 
    
    
    day
    
 as 
    
    day
    
, 
    
    
    external_website_conversions
    
 as 
    
    external_website_conversions
    
, 
    
    
    external_website_post_click_conversions
    
 as 
    
    external_website_post_click_conversions
    
, 
    
    
    external_website_post_view_conversions
    
 as 
    
    external_website_post_view_conversions
    
, 
    
    
    follows
    
 as 
    
    follows
    
, 
    
    
    full_screen_plays
    
 as 
    
    full_screen_plays
    
, 
    
    
    impressions
    
 as 
    
    impressions
    
, 
    
    
    landing_page_clicks
    
 as 
    
    landing_page_clicks
    
, 
    
    
    lead_generation_mail_contact_info_shares
    
 as 
    
    lead_generation_mail_contact_info_shares
    
, 
    
    
    lead_generation_mail_interested_clicks
    
 as 
    
    lead_generation_mail_interested_clicks
    
, 
    
    
    likes
    
 as 
    
    likes
    
, 
    
    
    one_click_lead_form_opens
    
 as 
    
    one_click_lead_form_opens
    
, 
    
    
    one_click_leads
    
 as 
    
    one_click_leads
    
, 
    
    
    opens
    
 as 
    
    opens
    
, 
    
    
    other_engagements
    
 as 
    
    other_engagements
    
, 
    
    
    shares
    
 as 
    
    shares
    
, 
    
    
    text_url_clicks
    
 as 
    
    text_url_clicks
    
, 
    
    
    total_engagements
    
 as 
    
    total_engagements
    
, 
    
    
    video_completions
    
 as 
    
    video_completions
    
, 
    
    
    video_first_quartile_completions
    
 as 
    
    video_first_quartile_completions
    
, 
    
    
    video_midpoint_completions
    
 as 
    
    video_midpoint_completions
    
, 
    
    
    video_starts
    
 as 
    
    video_starts
    
, 
    
    
    video_third_quartile_completions
    
 as 
    
    video_third_quartile_completions
    
, 
    
    
    video_views
    
 as 
    
    video_views
    
, 
    
    
    viral_card_clicks
    
 as 
    
    viral_card_clicks
    
, 
    
    
    viral_card_impressions
    
 as 
    
    viral_card_impressions
    
, 
    
    
    viral_clicks
    
 as 
    
    viral_clicks
    
, 
    
    
    viral_comment_likes
    
 as 
    
    viral_comment_likes
    
, 
    
    
    viral_comments
    
 as 
    
    viral_comments
    
, 
    
    
    viral_company_page_clicks
    
 as 
    
    viral_company_page_clicks
    
, 
    
    
    viral_external_website_conversions
    
 as 
    
    viral_external_website_conversions
    
, 
    
    
    viral_external_website_post_click_conversions
    
 as 
    
    viral_external_website_post_click_conversions
    
, 
    
    
    viral_external_website_post_view_conversions
    
 as 
    
    viral_external_website_post_view_conversions
    
, 
    cast(null as 
    int
) as 
    
    viral_extrernal_website_conversions
    
 , 
    cast(null as 
    int
) as 
    
    viral_extrernal_website_post_click_conversions
    
 , 
    cast(null as 
    int
) as 
    
    viral_extrernal_website_post_view_conversions
    
 , 
    
    
    viral_follows
    
 as 
    
    viral_follows
    
, 
    
    
    viral_full_screen_plays
    
 as 
    
    viral_full_screen_plays
    
, 
    
    
    viral_impressions
    
 as 
    
    viral_impressions
    
, 
    
    
    viral_landing_page_clicks
    
 as 
    
    viral_landing_page_clicks
    
, 
    
    
    viral_likes
    
 as 
    
    viral_likes
    
, 
    
    
    viral_one_click_lead_form_opens
    
 as 
    
    viral_one_click_lead_form_opens
    
, 
    
    
    viral_one_click_leads
    
 as 
    
    viral_one_click_leads
    
, 
    
    
    viral_other_engagements
    
 as 
    
    viral_other_engagements
    
, 
    
    
    viral_shares
    
 as 
    
    viral_shares
    
, 
    
    
    viral_total_engagements
    
 as 
    
    viral_total_engagements
    
, 
    
    
    viral_video_completions
    
 as 
    
    viral_video_completions
    
, 
    
    
    viral_video_first_quartile_completions
    
 as 
    
    viral_video_first_quartile_completions
    
, 
    
    
    viral_video_midpoint_completions
    
 as 
    
    viral_video_midpoint_completions
    
, 
    
    
    viral_video_starts
    
 as 
    
    viral_video_starts
    
, 
    
    
    viral_video_third_quartile_completions
    
 as 
    
    viral_video_third_quartile_completions
    
, 
    
    
    viral_video_views
    
 as 
    
    viral_video_views
    



    from base

), fields as (

    select
        creative_id,
        day as date_day,
        clicks, 
        impressions,
        
        cost_in_local_currency as cost
        
    from macro

), surrogate_key as (

    select
        *,
        md5(cast(
    
    coalesce(cast(date_day as 
    varchar
), '') || '-' || coalesce(cast(creative_id as 
    varchar
), '')

 as 
    varchar
)) as daily_creative_id
    from fields

)

select *
from surrogate_key