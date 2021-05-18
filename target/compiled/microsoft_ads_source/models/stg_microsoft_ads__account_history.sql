with base as (

    select *
    from DATAFLOTEST_DATABASE.bingads.account_history

), fields as (

    select 
        id as account_id,
        name as account_name,
        last_modified_time as modified_timestamp
    from base

), surrogate_key as (

    select 
        *,
        md5(cast(
    
    coalesce(cast(account_id as 
    varchar
), '') || '-' || coalesce(cast(modified_timestamp as 
    varchar
), '')

 as 
    varchar
)) as account_version_id
    from fields

), most_recent_record as (

    select
        *,
        row_number() over (partition by account_id order by modified_timestamp desc) = 1 as is_most_recent_version
    from surrogate_key

)

select *
from most_recent_record