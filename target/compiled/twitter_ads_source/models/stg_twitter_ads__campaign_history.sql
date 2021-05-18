with source as (

    select *
    from DATAFLOTEST_DATABASE.dbt_salesdataflo.stg_twitter_ads__campaign_history_tmp

),

renamed as (

    select
    
        
    
    
    _fivetran_synced
    
 as 
    
    _fivetran_synced
    
, 
    
    
    account_id
    
 as 
    
    account_id
    
, 
    cast(null as 
    timestamp_ntz
) as created_timestamp , 
    cast(null as 
    varchar
) as 
    
    currency
    
 , 
    cast(null as 
    int
) as 
    
    daily_budget_amount_local_micro
    
 , 
    cast(null as boolean) as is_deleted , 
    cast(null as 
    int
) as 
    
    duration_in_days
    
 , 
    cast(null as 
    timestamp_ntz
) as end_timestamp , 
    cast(null as 
    varchar
) as 
    
    entity_status
    
 , 
    cast(null as 
    int
) as 
    
    frequency_cap
    
 , 
    cast(null as 
    varchar
) as 
    
    funding_instrument_id
    
 , 
    
    
    id
    
 as campaign_id , 
    
    
    name
    
 as campaign_name , 
    cast(null as boolean) as is_servable , 
    cast(null as boolean) as 
    
    standard_delivery
    
 , 
    
    
    start_time
    
 as start_timestamp , 
    cast(null as 
    int
) as 
    
    total_budget_amount_local_micro
    
 , 
    cast(null as 
    timestamp_ntz
) as updated_timestamp 



    from source

), latest as (

    select
        *,
        row_number() over (partition by campaign_id order by updated_timestamp asc) = 1 as is_latest_version
    from renamed 

)

select * from latest