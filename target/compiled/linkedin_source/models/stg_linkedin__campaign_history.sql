with base as (

    select *
    from DATAFLOTEST_DATABASE.dbt_salesdataflo.stg_linkedin__campaign_history_tmp

), macro as (

    select 
        
    
    
    _fivetran_synced
    
 as 
    
    _fivetran_synced
    
, 
    
    
    account_id
    
 as 
    
    account_id
    
, 
    
    
    associated_entity
    
 as 
    
    associated_entity
    
, 
    
    
    audience_expansion_enabled
    
 as 
    
    audience_expansion_enabled
    
, 
    
    
    campaign_group_id
    
 as 
    
    campaign_group_id
    
, 
    
    
    cost_type
    
 as 
    
    cost_type
    
, 
    
    
    created_time
    
 as 
    
    created_time
    
, 
    
    
    creative_selection
    
 as 
    
    creative_selection
    
, 
    
    
    daily_budget_amount
    
 as 
    
    daily_budget_amount
    
, 
    
    
    daily_budget_currency_code
    
 as 
    
    daily_budget_currency_code
    
, 
    
    
    format
    
 as 
    
    format
    
, 
    
    
    id
    
 as 
    
    id
    
, 
    
    
    last_modified_time
    
 as 
    
    last_modified_time
    
, 
    
    
    locale_country
    
 as 
    
    locale_country
    
, 
    
    
    locale_language
    
 as 
    
    locale_language
    
, 
    
    
    name
    
 as 
    
    name
    
, 
    
    
    objective_type
    
 as 
    
    objective_type
    
, 
    
    
    offsite_delivery_enabled
    
 as 
    
    offsite_delivery_enabled
    
, 
    
    
    optimization_target_type
    
 as 
    
    optimization_target_type
    
, 
    
    
    run_schedule_end
    
 as 
    
    run_schedule_end
    
, 
    
    
    run_schedule_start
    
 as 
    
    run_schedule_start
    
, 
    
    
    status
    
 as 
    
    status
    
, 
    
    
    type
    
 as 
    
    type
    
, 
    
    
    unit_cost_amount
    
 as 
    
    unit_cost_amount
    
, 
    
    
    unit_cost_currency_code
    
 as 
    
    unit_cost_currency_code
    
, 
    
    
    version_tag
    
 as 
    
    version_tag
    



    from base

), fields as (

    select 
        id as campaign_id,
        last_modified_time as last_modified_at,
        account_id,
        campaign_group_id,
        created_time as created_at,
        name as campaign_name,
        cast(version_tag as numeric) as version_tag
    from macro

), valid_dates as (

    select 
        *,
        case 
            when row_number() over (partition by campaign_id order by version_tag) = 1 then created_at
            else last_modified_at
        end as valid_from,
        lead(last_modified_at) over (partition by campaign_id order by version_tag) as valid_to
    from fields

), surrogate_key as (

    select 
        *,
        md5(cast(
    
    coalesce(cast(campaign_id as 
    varchar
), '') || '-' || coalesce(cast(version_tag as 
    varchar
), '')

 as 
    varchar
)) as campaign_version_id
    from valid_dates

)

select *
from surrogate_key