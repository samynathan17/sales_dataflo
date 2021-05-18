with base as (

    select *
    from DATAFLOTEST_DATABASE.bingads.ad_group_history

), fields as (

    select 
        id as ad_group_id,
        campaign_id,
        name as ad_group_name,
        modified_time as modified_timestamp
    from base

), surrogate_key as (

    select 
        *,
        md5(cast(
    
    coalesce(cast(ad_group_id as 
    varchar
), '') || '-' || coalesce(cast(modified_timestamp as 
    varchar
), '')

 as 
    varchar
)) as ad_group_version_id
    from fields

), most_recent_record as (

    select
        *,
        row_number() over (partition by ad_group_id order by modified_timestamp desc) = 1 as is_most_recent_version
    from surrogate_key

)

select *
from most_recent_record