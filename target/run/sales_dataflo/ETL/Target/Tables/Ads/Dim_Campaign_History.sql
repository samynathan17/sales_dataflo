
  create or replace  view DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Campaign_History  as (
    with base as (

    select *
    from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Campaign_History
), fields as (

    select 
        id as campaign_id,
        last_modified_time as last_modified_at,
        account_id,
        campaign_group_id,
        created_time as created_at,
        name as campaign_name,
        cast(version_tag as numeric) as version_tag
    from base

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
  );
