with metrics as (

    select *
    from {{ ref('Stg_Ad_Analytics_By_Creative')}} 
), creatives as (

    select *
    from {{ ref('Dim_Creative_History') }}

), campaigns as (
    
    select *
    from {{ ref('Dim_Campaign_History') }}

), campaign_groups as (
    
    select *
    from {{ ref('Dim_Campaign_Group_History') }}

), accounts as (
    
    select *
    from {{ ref('Dim_Account_History') }}

), joined as (

    select
        metrics.creative_id,
        metrics.day as date_day,
        metrics.clicks,
        metrics.impressions,
        metrics.cost_in_local_currency as cost,
        metrics.daily_creative_id,
        campaigns.campaign_name,
        campaigns.campaign_id,
        campaign_groups.campaign_group_name,
        campaign_groups.campaign_group_id,
        accounts.account_name,
        accounts.account_id,
        metrics.source_type
    from metrics
    left join creatives
        on metrics.creative_id = creatives.creative_id
        and {{ fivetran_utils.timestamp_add('day', 1, 'metrics.day') }} >= creatives.valid_from
        and {{ fivetran_utils.timestamp_add('day', 1, 'metrics.day') }} <= coalesce(creatives.valid_to, {{ fivetran_utils.timestamp_add('day', 1, dbt_utils.current_timestamp()) }})
    left join campaigns
        on creatives.campaign_id = campaigns.campaign_id
        and {{ fivetran_utils.timestamp_add('day', 1, 'metrics.day') }} >= campaigns.valid_from
        and {{ fivetran_utils.timestamp_add('day', 1, 'metrics.day') }} <= coalesce(campaigns.valid_to, {{ fivetran_utils.timestamp_add('day', 1, dbt_utils.current_timestamp()) }})
    left join campaign_groups
        on campaigns.campaign_group_id = campaign_groups.campaign_group_id
        and {{ fivetran_utils.timestamp_add('day', 1, 'metrics.day') }} >= campaign_groups.valid_from
        and {{ fivetran_utils.timestamp_add('day', 1, 'metrics.day') }} <= coalesce(campaign_groups.valid_to, {{ fivetran_utils.timestamp_add('day', 1, dbt_utils.current_timestamp()) }})
    left join accounts
        on campaign_groups.account_id = accounts.account_id
        and {{ fivetran_utils.timestamp_add('day', 1, 'metrics.day') }} >= accounts.valid_from
        and {{ fivetran_utils.timestamp_add('day', 1, 'metrics.day') }} <= coalesce(accounts.valid_to, {{ fivetran_utils.timestamp_add('day', 1, dbt_utils.current_timestamp()) }})

)

select *
from joined