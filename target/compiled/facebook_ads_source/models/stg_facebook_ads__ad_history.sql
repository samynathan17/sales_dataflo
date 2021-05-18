with base as (

    select * 
    from DATAFLOTEST_DATABASE.dbt_salesdataflo.stg_facebook_ads__ad_history_tmp

),

fields as (

    select
        
    
    
    _fivetran_synced
    
 as 
    
    _fivetran_synced
    
, 
    
    
    account_id
    
 as 
    
    account_id
    
, 
    
    
    ad_set_id
    
 as 
    
    ad_set_id
    
, 
    
    
    ad_source_id
    
 as 
    
    ad_source_id
    
, 
    
    
    bid_amount
    
 as 
    
    bid_amount
    
, 
    
    
    bid_info_actions
    
 as 
    
    bid_info_actions
    
, 
    
    
    bid_type
    
 as 
    
    bid_type
    
, 
    
    
    campaign_id
    
 as 
    
    campaign_id
    
, 
    
    
    configured_status
    
 as 
    
    configured_status
    
, 
    
    
    created_time
    
 as 
    
    created_time
    
, 
    
    
    creative_id
    
 as 
    
    creative_id
    
, 
    
    
    effective_status
    
 as 
    
    effective_status
    
, 
    cast(null as 
    varchar
) as 
    
    global_discriminatory_practices
    
 , 
    cast(null as 
    varchar
) as 
    
    global_non_functional_landing_page
    
 , 
    cast(null as 
    varchar
) as 
    
    global_use_of_our_brand_assets
    
 , 
    
    
    id
    
 as 
    
    id
    
, 
    
    
    last_updated_by_app_id
    
 as 
    
    last_updated_by_app_id
    
, 
    
    
    name
    
 as 
    
    name
    
, 
    cast(null as 
    varchar
) as 
    
    placement_specific_facebook_discriminatory_practices
    
 , 
    cast(null as 
    varchar
) as 
    
    placement_specific_facebook_non_functional_landing_page
    
 , 
    cast(null as 
    varchar
) as 
    
    placement_specific_facebook_use_of_our_brand_assets
    
 , 
    cast(null as 
    varchar
) as 
    
    placement_specific_instagram_discriminatory_practices
    
 , 
    
    
    status
    
 as 
    
    status
    
, 
    
    
    updated_time
    
 as 
    
    updated_time
    



        
    from base
),

fields_xf as (
    
    select 
        id as ad_id,
        account_id,
        ad_set_id,
        campaign_id,
        creative_id,
        name as ad_name,
        row_number() over (partition by id order by _fivetran_synced desc) = 1 as is_most_recent_record
    from fields

)

select * from fields_xf