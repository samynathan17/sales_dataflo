with base as (

    select *
    from DATAFLOTEST_DATABASE.bingads.campaign_history

), fields as (

    select 
        id as campaign_id,
        account_id,
        name as campaign_name,
        modified_time as modified_timestamp
    from base

), surrogate_key as (

    select 
        *,
        md5(cast(
    
    coalesce(cast(campaign_id as 
    varchar
), '') || '-' || coalesce(cast(modified_timestamp as 
    varchar
), '')

 as 
    varchar
)) as campaign_version_id
    from fields

), most_recent_record as (

    select
        *,
        row_number() over (partition by campaign_id order by modified_timestamp desc) = 1 as is_most_recent_version
    from surrogate_key

)

select *
from most_recent_record