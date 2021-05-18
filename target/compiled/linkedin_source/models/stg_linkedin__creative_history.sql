with base as (

    select *
    from DATAFLOTEST_DATABASE.dbt_salesdataflo.stg_linkedin__creative_history_tmp

), macro as (

    select 
        
    
    
    _fivetran_synced
    
 as 
    
    _fivetran_synced
    
, 
    
    
    call_to_action_label_type
    
 as 
    
    call_to_action_label_type
    
, 
    
    
    call_to_action_target
    
 as 
    
    call_to_action_target
    
, 
    
    
    campaign_id
    
 as 
    
    campaign_id
    
, 
    
    
    click_uri
    
 as 
    
    click_uri
    
, 
    
    
    created_time
    
 as 
    
    created_time
    
, 
    
    
    follow_company_call_to_action
    
 as 
    
    follow_company_call_to_action
    
, 
    
    
    follow_company_organization_logo
    
 as 
    
    follow_company_organization_logo
    
, 
    
    
    follow_company_organization_name
    
 as 
    
    follow_company_organization_name
    
, 
    
    
    id
    
 as 
    
    id
    
, 
    
    
    jobs_company_name
    
 as 
    
    jobs_company_name
    
, 
    
    
    jobs_logo
    
 as 
    
    jobs_logo
    
, 
    
    
    jobs_organization
    
 as 
    
    jobs_organization
    
, 
    
    
    last_modified_time
    
 as 
    
    last_modified_time
    
, 
    
    
    reference
    
 as 
    
    reference
    
, 
    
    
    review_status
    
 as 
    
    review_status
    
, 
    
    
    sponsored_in_mail_content
    
 as 
    
    sponsored_in_mail_content
    
, 
    
    
    sponsored_update_activity
    
 as 
    
    sponsored_update_activity
    
, 
    
    
    sponsored_update_carousel_activity
    
 as 
    
    sponsored_update_carousel_activity
    
, 
    
    
    sponsored_update_carousel_direct_sponsored_content
    
 as 
    
    sponsored_update_carousel_direct_sponsored_content
    
, 
    
    
    sponsored_update_carousel_share
    
 as 
    
    sponsored_update_carousel_share
    
, 
    
    
    sponsored_update_carousel_share_content_description
    
 as 
    
    sponsored_update_carousel_share_content_description
    
, 
    
    
    sponsored_update_carousel_share_content_share_media_category
    
 as 
    
    sponsored_update_carousel_share_content_share_media_category
    
, 
    
    
    sponsored_update_carousel_share_content_title
    
 as 
    
    sponsored_update_carousel_share_content_title
    
, 
    
    
    sponsored_update_carousel_share_subject
    
 as 
    
    sponsored_update_carousel_share_subject
    
, 
    
    
    sponsored_update_direct_sponsored_content
    
 as 
    
    sponsored_update_direct_sponsored_content
    
, 
    
    
    sponsored_update_share
    
 as 
    
    sponsored_update_share
    
, 
    
    
    sponsored_update_share_content_description
    
 as 
    
    sponsored_update_share_content_description
    
, 
    
    
    sponsored_update_share_content_share_media_category
    
 as 
    
    sponsored_update_share_content_share_media_category
    
, 
    
    
    sponsored_update_share_content_title
    
 as 
    
    sponsored_update_share_content_title
    
, 
    
    
    sponsored_update_share_subject
    
 as 
    
    sponsored_update_share_subject
    
, 
    
    
    sponsored_video_media_asset
    
 as 
    
    sponsored_video_media_asset
    
, 
    
    
    sponsored_video_user_generated_content_post
    
 as 
    
    sponsored_video_user_generated_content_post
    
, 
    
    
    spotlight_call_to_action
    
 as 
    
    spotlight_call_to_action
    
, 
    
    
    spotlight_custom_background
    
 as 
    
    spotlight_custom_background
    
, 
    
    
    spotlight_description
    
 as 
    
    spotlight_description
    
, 
    
    
    spotlight_forum_name
    
 as 
    
    spotlight_forum_name
    
, 
    
    
    spotlight_headline
    
 as 
    
    spotlight_headline
    
, 
    
    
    spotlight_logo
    
 as 
    
    spotlight_logo
    
, 
    
    
    spotlight_show_member_profile_photo
    
 as 
    
    spotlight_show_member_profile_photo
    
, 
    
    
    status
    
 as 
    
    status
    
, 
    
    
    text_ad_text
    
 as 
    
    text_ad_text
    
, 
    
    
    text_ad_title
    
 as 
    
    text_ad_title
    
, 
    
    
    type
    
 as 
    
    type
    
, 
    
    
    version_tag
    
 as 
    
    version_tag
    



    from base

), fields as (

    select
        id as creative_id,
        last_modified_time as last_modified_at,
        created_time as created_at,
        campaign_id,
        type as creative_type,
        cast(version_tag as numeric) as version_tag,
        status as creative_status,
        click_uri
    from macro

), url_fields as (

    select 
        *,
        
  

    split_part(
        click_uri,
        '?',
        1
        )


 as base_url,
        
  
    try_cast(
  

    split_part(
        
  

    split_part(
        

    replace(
        

    replace(
        click_uri,
        'http://',
        ''
    )
    


,
        'https://',
        ''
    )
    


,
        '/',
        1
        )


,
        '?',
        1
        )


 as 
    varchar
)

 as url_host,
        '/' || 
  
    try_cast(
  

    split_part(
        

    case when 
    
    length(
        

    replace(
        

    replace(
        click_uri,
        'http://',
        ''
    )
    


,
        'https://',
        ''
    )
    



    )
-coalesce(
            nullif(

    position(
        '/' in 

    replace(
        

    replace(
        click_uri,
        'http://',
        ''
    )
    


,
        'https://',
        ''
    )
    



    )
, 0),
            

    position(
        '?' in 

    replace(
        

    replace(
        click_uri,
        'http://',
        ''
    )
    


,
        'https://',
        ''
    )
    



    )
 - 1
            ) = 0 
        then ''
    else 
        right(
            

    replace(
        

    replace(
        click_uri,
        'http://',
        ''
    )
    


,
        'https://',
        ''
    )
    


,
            
    
    length(
        

    replace(
        

    replace(
        click_uri,
        'http://',
        ''
    )
    


,
        'https://',
        ''
    )
    



    )
-coalesce(
            nullif(

    position(
        '/' in 

    replace(
        

    replace(
        click_uri,
        'http://',
        ''
    )
    


,
        'https://',
        ''
    )
    



    )
, 0),
            

    position(
        '?' in 

    replace(
        

    replace(
        click_uri,
        'http://',
        ''
    )
    


,
        'https://',
        ''
    )
    



    )
 - 1
            )
        )
    end
,
        '?',
        1
        )


 as 
    varchar
)

 as url_path,
        nullif(
  

    split_part(
        
  

    split_part(
        click_uri,
        'utm_source=',
        2
        )


,
        '&',
        1
        )


,'') as utm_source,
        nullif(
  

    split_part(
        
  

    split_part(
        click_uri,
        'utm_medium=',
        2
        )


,
        '&',
        1
        )


,'') as utm_medium,
        nullif(
  

    split_part(
        
  

    split_part(
        click_uri,
        'utm_campaign=',
        2
        )


,
        '&',
        1
        )


,'') as utm_campaign,
        nullif(
  

    split_part(
        
  

    split_part(
        click_uri,
        'utm_content=',
        2
        )


,
        '&',
        1
        )


,'') as utm_content,
        nullif(
  

    split_part(
        
  

    split_part(
        click_uri,
        'utm_term=',
        2
        )


,
        '&',
        1
        )


,'') as utm_term
    from fields

), valid_dates as (

    select 
        *,
        case 
            when row_number() over (partition by creative_id order by version_tag) = 1 then created_at
            else last_modified_at
        end as valid_from,
        lead(last_modified_at) over (partition by creative_id order by version_tag) as valid_to
    from url_fields

), surrogate_key as (

    select 
        *,
        md5(cast(
    
    coalesce(cast(creative_id as 
    varchar
), '') || '-' || coalesce(cast(version_tag as 
    varchar
), '')

 as 
    varchar
)) as creative_version_id
    from valid_dates

)

select *
from surrogate_key