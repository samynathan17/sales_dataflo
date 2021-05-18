
  create or replace  view DATAFLOTEST_DATABASE.dbt_salesdataflo.stg_twitter_ads__account_history  as (
    with source as (

    select *
    from DATAFLOTEST_DATABASE.dbt_salesdataflo.stg_twitter_ads__account_history_tmp

),

renamed as (

    select
    
        
    
    
    _fivetran_synced
    
 as 
    
    _fivetran_synced
    
, 
    cast(null as 
    varchar
) as 
    
    approval_status
    
 , 
    cast(null as 
    varchar
) as 
    
    business_id
    
 , 
    
    
    business_name
    
 as 
    
    business_name
    
, 
    cast(null as 
    timestamp_ntz
) as created_timestamp , 
    cast(null as boolean) as is_deleted , 
    
    
    id
    
 as account_id , 
    cast(null as 
    varchar
) as 
    
    industry_type
    
 , 
    
    
    name
    
 as 
    
    name
    
, 
    cast(null as 
    varchar
) as 
    
    salt
    
 , 
    cast(null as 
    varchar
) as 
    
    timezone
    
 , 
    cast(null as 
    timestamp_ntz
) as 
    
    timezone_switch_at
    
 , 
    cast(null as 
    timestamp_ntz
) as updated_timestamp 



    from source

), latest as (

    select
        *,
        row_number() over (partition by account_id order by updated_timestamp asc) = 1 as is_latest_version
    from renamed 

)

select * from latest
  );
