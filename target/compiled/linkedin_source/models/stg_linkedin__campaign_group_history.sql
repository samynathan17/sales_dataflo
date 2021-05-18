with base as (

    select *
    from DATAFLOTEST_DATABASE.dbt_salesdataflo.stg_linkedin__campaign_group_history_tmp

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
    
    
    backfilled
    
 as 
    
    backfilled
    
, 
    
    
    created_time
    
 as 
    
    created_time
    
, 
    
    
    id
    
 as 
    
    id
    
, 
    
    
    last_modified_time
    
 as 
    
    last_modified_time
    
, 
    
    
    name
    
 as 
    
    name
    
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
    



    from base

), fields as (

    select 
        id as campaign_group_id,
        last_modified_time as last_modified_at,
        account_id,
        created_time as created_at,
        name as campaign_group_name
    from macro

), valid_dates as (

    select 
        *,
        case 
            when row_number() over (partition by campaign_group_id order by last_modified_at) = 1 then created_at
            else last_modified_at
        end as valid_from,
        lead(last_modified_at) over (partition by campaign_group_id order by last_modified_at) as valid_to
    from fields

), surrogate_key as (

    select 
        *,
        md5(cast(
    
    coalesce(cast(campaign_group_id as 
    varchar
), '') || '-' || coalesce(cast(last_modified_at as 
    varchar
), '')

 as 
    varchar
)) as campaign_group_version_id
    from valid_dates

)

select *
from surrogate_key