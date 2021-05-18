

      create or replace transient table DATAFLOTEST_DATABASE.dbt_salesdataflo.stg_facebook_ads__ad_set_history  as
      (with base as (

    select * 
    from DATAFLOTEST_DATABASE.dbt_salesdataflo.stg_facebook_ads__ad_set_history_tmp

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
    
    
    adset_source_id
    
 as 
    
    adset_source_id
    
, 
    
    
    bid_amount
    
 as 
    
    bid_amount
    
, 
    
    
    bid_info_actions
    
 as 
    
    bid_info_actions
    
, 
    
    
    bid_strategy
    
 as 
    
    bid_strategy
    
, 
    
    
    billing_event
    
 as 
    
    billing_event
    
, 
    
    
    budget_remaining
    
 as 
    
    budget_remaining
    
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
    
    
    daily_budget
    
 as 
    
    daily_budget
    
, 
    
    
    destination_type
    
 as 
    
    destination_type
    
, 
    
    
    effective_status
    
 as 
    
    effective_status
    
, 
    
    
    end_time
    
 as 
    
    end_time
    
, 
    
    
    id
    
 as 
    
    id
    
, 
    
    
    instagram_actor_id
    
 as 
    
    instagram_actor_id
    
, 
    
    
    lifetime_budget
    
 as 
    
    lifetime_budget
    
, 
    
    
    lifetime_imps
    
 as 
    
    lifetime_imps
    
, 
    
    
    name
    
 as 
    
    name
    
, 
    
    
    optimization_goal
    
 as 
    
    optimization_goal
    
, 
    
    
    promoted_object_application_id
    
 as 
    
    promoted_object_application_id
    
, 
    
    
    promoted_object_custom_event_type
    
 as 
    
    promoted_object_custom_event_type
    
, 
    
    
    promoted_object_event_id
    
 as 
    
    promoted_object_event_id
    
, 
    
    
    promoted_object_object_store_url
    
 as 
    
    promoted_object_object_store_url
    
, 
    
    
    promoted_object_offer_id
    
 as 
    
    promoted_object_offer_id
    
, 
    
    
    promoted_object_page_id
    
 as 
    
    promoted_object_page_id
    
, 
    
    
    promoted_object_pixel_id
    
 as 
    
    promoted_object_pixel_id
    
, 
    
    
    promoted_object_place_page_set_id
    
 as 
    
    promoted_object_place_page_set_id
    
, 
    
    
    promoted_object_product_catalog_id
    
 as 
    
    promoted_object_product_catalog_id
    
, 
    
    
    promoted_object_product_set_id
    
 as 
    
    promoted_object_product_set_id
    
, 
    
    
    recurring_budget_semantics
    
 as 
    
    recurring_budget_semantics
    
, 
    
    
    rf_prediction_id
    
 as 
    
    rf_prediction_id
    
, 
    
    
    start_time
    
 as 
    
    start_time
    
, 
    
    
    status
    
 as 
    
    status
    
, 
    
    
    targeting_age_max
    
 as 
    
    targeting_age_max
    
, 
    
    
    targeting_age_min
    
 as 
    
    targeting_age_min
    
, 
    
    
    targeting_app_install_state
    
 as 
    
    targeting_app_install_state
    
, 
    
    
    targeting_audience_network_positions
    
 as 
    
    targeting_audience_network_positions
    
, 
    
    
    targeting_college_years
    
 as 
    
    targeting_college_years
    
, 
    
    
    targeting_connections
    
 as 
    
    targeting_connections
    
, 
    
    
    targeting_device_platforms
    
 as 
    
    targeting_device_platforms
    
, 
    
    
    targeting_education_majors
    
 as 
    
    targeting_education_majors
    
, 
    
    
    targeting_education_schools
    
 as 
    
    targeting_education_schools
    
, 
    
    
    targeting_education_statuses
    
 as 
    
    targeting_education_statuses
    
, 
    
    
    targeting_effective_audience_network_positions
    
 as 
    
    targeting_effective_audience_network_positions
    
, 
    
    
    targeting_excluded_connections
    
 as 
    
    targeting_excluded_connections
    
, 
    
    
    targeting_excluded_publisher_categories
    
 as 
    
    targeting_excluded_publisher_categories
    
, 
    
    
    targeting_excluded_publisher_list_ids
    
 as 
    
    targeting_excluded_publisher_list_ids
    
, 
    
    
    targeting_excluded_user_device
    
 as 
    
    targeting_excluded_user_device
    
, 
    
    
    targeting_exclusions
    
 as 
    
    targeting_exclusions
    
, 
    
    
    targeting_facebook_positions
    
 as 
    
    targeting_facebook_positions
    
, 
    
    
    targeting_flexible_spec
    
 as 
    
    targeting_flexible_spec
    
, 
    
    
    targeting_friends_of_connections
    
 as 
    
    targeting_friends_of_connections
    
, 
    
    
    targeting_geo_locations_countries
    
 as 
    
    targeting_geo_locations_countries
    
, 
    
    
    targeting_geo_locations_location_types
    
 as 
    
    targeting_geo_locations_location_types
    
, 
    
    
    targeting_instagram_positions
    
 as 
    
    targeting_instagram_positions
    
, 
    
    
    targeting_locales
    
 as 
    
    targeting_locales
    
, 
    
    
    targeting_publisher_platforms
    
 as 
    
    targeting_publisher_platforms
    
, 
    
    
    targeting_user_adclusters
    
 as 
    
    targeting_user_adclusters
    
, 
    
    
    targeting_user_device
    
 as 
    
    targeting_user_device
    
, 
    
    
    targeting_user_os
    
 as 
    
    targeting_user_os
    
, 
    
    
    targeting_wireless_carrier
    
 as 
    
    targeting_wireless_carrier
    
, 
    
    
    targeting_work_employers
    
 as 
    
    targeting_work_employers
    
, 
    
    
    targeting_work_positions
    
 as 
    
    targeting_work_positions
    
, 
    
    
    updated_time
    
 as 
    
    updated_time
    
, 
    
    
    use_new_app_click
    
 as 
    
    use_new_app_click
    



        
    from base
),

fields_xf as (
    
    select 
        id as ad_set_id,
        account_id,
        campaign_id,
        name as ad_set_name,
        row_number() over (partition by id order by _fivetran_synced desc) = 1 as is_most_recent_record
    from fields

)

select * from fields_xf
      );
    