

      create or replace transient table DATAFLOTEST_DATABASE.dbt_salesdataflo.stg_facebook_ads__creative_history  as
      (with base as (

    select * 
    from DATAFLOTEST_DATABASE.dbt_salesdataflo.stg_facebook_ads__creative_history_tmp

),

fields as (

    select
        
    
    
    _fivetran_id
    
 as 
    
    _fivetran_id
    
, 
    
    
    _fivetran_synced
    
 as 
    
    _fivetran_synced
    
, 
    
    
    account_id
    
 as 
    
    account_id
    
, 
    
    
    actor_id
    
 as 
    
    actor_id
    
, 
    
    
    applink_treatment
    
 as 
    
    applink_treatment
    
, 
    
    
    asset_feed_spec_link_urls
    
 as 
    
    asset_feed_spec_link_urls
    
, 
    
    
    body
    
 as 
    
    body
    
, 
    
    
    branded_content_sponsor_page_id
    
 as 
    
    branded_content_sponsor_page_id
    
, 
    
    
    call_to_action_type
    
 as 
    
    call_to_action_type
    
, 
    
    
    carousel_ad_link
    
 as 
    
    carousel_ad_link
    
, 
    
    
    effective_instagram_story_id
    
 as 
    
    effective_instagram_story_id
    
, 
    
    
    effective_object_story_id
    
 as 
    
    effective_object_story_id
    
, 
    
    
    id
    
 as 
    
    id
    
, 
    
    
    image_file
    
 as 
    
    image_file
    
, 
    
    
    image_hash
    
 as 
    
    image_hash
    
, 
    
    
    image_url
    
 as 
    
    image_url
    
, 
    
    
    instagram_actor_id
    
 as 
    
    instagram_actor_id
    
, 
    
    
    instagram_permalink_url
    
 as 
    
    instagram_permalink_url
    
, 
    
    
    instagram_story_id
    
 as 
    
    instagram_story_id
    
, 
    
    
    link_og_id
    
 as 
    
    link_og_id
    
, 
    
    
    link_url
    
 as 
    
    link_url
    
, 
    
    
    name
    
 as 
    
    name
    
, 
    
    
    object_id
    
 as 
    
    object_id
    
, 
    
    
    object_story_id
    
 as 
    
    object_story_id
    
, 
    
    
    object_story_link_data_app_link_spec_android
    
 as 
    
    object_story_link_data_app_link_spec_android
    
, 
    
    
    object_story_link_data_app_link_spec_ios
    
 as 
    
    object_story_link_data_app_link_spec_ios
    
, 
    
    
    object_story_link_data_app_link_spec_ipad
    
 as 
    
    object_story_link_data_app_link_spec_ipad
    
, 
    
    
    object_story_link_data_app_link_spec_iphone
    
 as 
    
    object_story_link_data_app_link_spec_iphone
    
, 
    
    
    object_story_link_data_caption
    
 as 
    
    object_story_link_data_caption
    
, 
    
    
    object_story_link_data_child_attachments
    
 as 
    
    object_story_link_data_child_attachments
    
, 
    
    
    object_story_link_data_description
    
 as 
    
    object_story_link_data_description
    
, 
    
    
    object_story_link_data_link
    
 as 
    
    object_story_link_data_link
    
, 
    
    
    object_story_link_data_message
    
 as 
    
    object_story_link_data_message
    
, 
    
    
    object_type
    
 as 
    
    object_type
    
, 
    
    
    object_url
    
 as 
    
    object_url
    
, 
    
    
    page_link
    
 as 
    
    page_link
    
, 
    
    
    page_message
    
 as 
    
    page_message
    
, 
    
    
    product_set_id
    
 as 
    
    product_set_id
    
, 
    
    
    status
    
 as 
    
    status
    
, 
    
    
    template_app_link_spec_android
    
 as 
    
    template_app_link_spec_android
    
, 
    
    
    template_app_link_spec_ios
    
 as 
    
    template_app_link_spec_ios
    
, 
    
    
    template_app_link_spec_ipad
    
 as 
    
    template_app_link_spec_ipad
    
, 
    
    
    template_app_link_spec_iphone
    
 as 
    
    template_app_link_spec_iphone
    
, 
    
    
    template_caption
    
 as 
    
    template_caption
    
, 
    
    
    template_child_attachments
    
 as 
    
    template_child_attachments
    
, 
    
    
    template_description
    
 as 
    
    template_description
    
, 
    
    
    template_link
    
 as 
    
    template_link
    
, 
    
    
    template_message
    
 as 
    
    template_message
    
, 
    
    
    template_page_link
    
 as 
    
    template_page_link
    
, 
    
    
    template_url
    
 as 
    
    template_url
    
, 
    
    
    thumbnail_url
    
 as 
    
    thumbnail_url
    
, 
    
    
    title
    
 as 
    
    title
    
, 
    
    
    url_tags
    
 as 
    
    url_tags
    
, 
    
    
    use_page_actor_override
    
 as 
    
    use_page_actor_override
    
, 
    
    
    video_call_to_action_value_link
    
 as 
    
    video_call_to_action_value_link
    
, 
    
    
    video_id
    
 as 
    
    video_id
    



        
    from base
),

fields_xf as (
    
    select 
        _fivetran_id,
        id as creative_id,
        account_id,
        name as creative_name,
        page_link,
        template_page_link,
        url_tags,
        asset_feed_spec_link_urls,
        object_story_link_data_child_attachments,
        object_story_link_data_caption, 
        object_story_link_data_description, 
        object_story_link_data_link, 
        object_story_link_data_message,
        template_app_link_spec_ios,
        template_app_link_spec_ipad,
        template_app_link_spec_android,
        template_app_link_spec_iphone,
        row_number() over (partition by id order by _fivetran_synced desc) = 1 as is_most_recent_record
    from fields
    
)

select * from fields_xf
      );
    