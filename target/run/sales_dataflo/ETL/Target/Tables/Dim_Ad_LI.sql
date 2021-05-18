
  create or replace  view DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Ad_LI  as (
    with metrics as (

    select *
    from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Ad_Analytics_By_Creative 
), creatives as (

    select *
    from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Creative_History

), campaigns as (
    
    select *
    from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Campaign_History

), campaign_groups as (
    
    select *
    from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Campaign_Group_History

), accounts as (
    
    select *
    from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Account_History

), joined as (

    select
        metrics.creative_id,
        metrics.day as date_day,
        metrics.clicks,
        metrics.impressions,
        metrics.cost,
        metrics.daily_creative_id,
        campaigns.campaign_name,
        campaigns.campaign_id,
        campaign_groups.campaign_group_name,
        campaign_groups.campaign_group_id,
        accounts.account_name,
        accounts.account_id
    from metrics
    left join creatives
        on metrics.creative_id = creatives.creative_id
        and 

    timestampadd(
        day,
        1,
        metrics.date_day
        )

 >= creatives.valid_from
        and 

    timestampadd(
        day,
        1,
        metrics.date_day
        )

 <= coalesce(creatives.valid_to, 

    timestampadd(
        day,
        1,
        
    current_timestamp::
    timestamp_ntz


        )

)
    left join campaigns
        on creatives.campaign_id = campaigns.campaign_id
        and 

    timestampadd(
        day,
        1,
        metrics.date_day
        )

 >= campaigns.valid_from
        and 

    timestampadd(
        day,
        1,
        metrics.date_day
        )

 <= coalesce(campaigns.valid_to, 

    timestampadd(
        day,
        1,
        
    current_timestamp::
    timestamp_ntz


        )

)
    left join campaign_groups
        on campaigns.campaign_group_id = campaign_groups.campaign_group_id
        and 

    timestampadd(
        day,
        1,
        metrics.date_day
        )

 >= campaign_groups.valid_from
        and 

    timestampadd(
        day,
        1,
        metrics.date_day
        )

 <= coalesce(campaign_groups.valid_to, 

    timestampadd(
        day,
        1,
        
    current_timestamp::
    timestamp_ntz


        )

)
    left join accounts
        on campaign_groups.account_id = accounts.account_id
        and 

    timestampadd(
        day,
        1,
        metrics.date_day
        )

 >= accounts.valid_from
        and 

    timestampadd(
        day,
        1,
        metrics.date_day
        )

 <= coalesce(accounts.valid_to, 

    timestampadd(
        day,
        1,
        
    current_timestamp::
    timestamp_ntz


        )

)

)

select *
from joined
  );
