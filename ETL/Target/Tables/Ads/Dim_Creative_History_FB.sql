with base as (

    select * 
    from {{ ref('Stg_Creative_History_FB') }}

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
    from base
    
)

select * from fields_xf
