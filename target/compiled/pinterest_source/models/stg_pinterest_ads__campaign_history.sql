with base as (

    select *
    from DATAFLOTEST_DATABASE.dbt_salesdataflo.stg_pinterest_ads__campaign_history_tmp

), fields as (

    select
        
    
    
    _fivetran_synced
    
 as 
    
    _fivetran_synced
    
, 
    
    
    created_time
    
 as created_timestamp , 
    
    
    id
    
 as campaign_id , 
    
    
    name
    
 as 
    
    name
    
, 
    
    
    status
    
 as 
    
    status
    



    from base

), surrogate_key as (

    select 
        *,
        md5(cast(
    
    coalesce(cast(campaign_id as 
    varchar
), '') || '-' || coalesce(cast(_fivetran_synced as 
    varchar
), '')

 as 
    varchar
)) as version_id
    from fields

)

select *
from surrogate_key