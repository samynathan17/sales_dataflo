

      create or replace transient table DATAFLOTEST_DATABASE.dbt_salesdataflo.stg_facebook_ads__campaign_history  as
      (with base as (

    select * 
    from DATAFLOTEST_DATABASE.dbt_salesdataflo.stg_facebook_ads__campaign_history_tmp

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
    
    
    boosted_object_id
    
 as 
    
    boosted_object_id
    
, 
    
    
    budget_rebalance_flag
    
 as 
    
    budget_rebalance_flag
    
, 
    
    
    buying_type
    
 as 
    
    buying_type
    
, 
    
    
    can_create_brand_lift_study
    
 as 
    
    can_create_brand_lift_study
    
, 
    
    
    can_use_spend_cap
    
 as 
    
    can_use_spend_cap
    
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
    
    
    effective_status
    
 as 
    
    effective_status
    
, 
    
    
    id
    
 as 
    
    id
    
, 
    
    
    name
    
 as 
    
    name
    
, 
    
    
    objective
    
 as 
    
    objective
    
, 
    
    
    source_campaign_id
    
 as 
    
    source_campaign_id
    
, 
    
    
    spend_cap
    
 as 
    
    spend_cap
    
, 
    
    
    start_time
    
 as 
    
    start_time
    
, 
    
    
    status
    
 as 
    
    status
    
, 
    
    
    stop_time
    
 as 
    
    stop_time
    
, 
    
    
    updated_time
    
 as 
    
    updated_time
    



        
    from base
),

fields_xf as (
    
    select 
        id as campaign_id,
        account_id,
        name as campaign_name,
        row_number() over (partition by id order by _fivetran_synced desc) = 1 as is_most_recent_record
    from fields

)

select * from fields_xf
      );
    