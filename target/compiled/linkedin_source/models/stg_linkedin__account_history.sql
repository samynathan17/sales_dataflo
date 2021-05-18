with base as (

    select *
    from DATAFLOTEST_DATABASE.dbt_salesdataflo.stg_linkedin__account_history_tmp

), macro as (

    select
        
    
    
    _fivetran_synced
    
 as 
    
    _fivetran_synced
    
, 
    
    
    created_time
    
 as 
    
    created_time
    
, 
    
    
    currency
    
 as 
    
    currency
    
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
    
    
    notified_on_campaign_optimization
    
 as 
    
    notified_on_campaign_optimization
    
, 
    
    
    notified_on_creative_approval
    
 as 
    
    notified_on_creative_approval
    
, 
    
    
    notified_on_creative_rejection
    
 as 
    
    notified_on_creative_rejection
    
, 
    
    
    notified_on_end_of_campaign
    
 as 
    
    notified_on_end_of_campaign
    
, 
    
    
    reference
    
 as 
    
    reference
    
, 
    
    
    status
    
 as 
    
    status
    
, 
    cast(null as 
    float
) as 
    
    total_budget_amount
    
 , 
    cast(null as 
    varchar
) as 
    
    total_budget_currency_code
    
 , 
    cast(null as 
    int
) as 
    
    total_budget_ends_at
    
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
        id as account_id,
        last_modified_time as last_modified_at,
        created_time as created_at,
        name as account_name,
        currency,
        cast(version_tag as numeric) as version_tag
    from macro

), valid_dates as (

    select 
        *,
        case 
            when row_number() over (partition by account_id order by version_tag) = 1 then created_at
            else last_modified_at
        end as valid_from,
        lead(last_modified_at) over (partition by account_id order by version_tag) as valid_to
    from fields

), surrogate_key as (

    select 
        *,
        md5(cast(
    
    coalesce(cast(account_id as 
    varchar
), '') || '-' || coalesce(cast(version_tag as 
    varchar
), '')

 as 
    varchar
)) as account_version_id
    from valid_dates

)

select *
from surrogate_key