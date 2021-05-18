

      create or replace transient table DATAFLOTEST_DATABASE.dbt_salesdataflo.facebook_ads__ad_adapter  as
      (with report as (

    select *
    from DATAFLOTEST_DATABASE.dbt_salesdataflo.stg_facebook_ads__basic_ad

), creatives as (

    select *
    from DATAFLOTEST_DATABASE.dbt_salesdataflo.facebook_ads__creative_history_prep

), accounts as (

    select *
    from DATAFLOTEST_DATABASE.dbt_salesdataflo.stg_facebook_ads__account_history
    where is_most_recent_record = true

), ads as (

    select *
    from DATAFLOTEST_DATABASE.dbt_salesdataflo.stg_facebook_ads__ad_history
    where is_most_recent_record = true

), ad_sets as (

    select *
    from DATAFLOTEST_DATABASE.dbt_salesdataflo.stg_facebook_ads__ad_set_history
    where is_most_recent_record = true

), campaigns as (

    select *
    from DATAFLOTEST_DATABASE.dbt_salesdataflo.stg_facebook_ads__campaign_history
    where is_most_recent_record = true

), joined as (

    select
        report.date_day,
        accounts.account_id,
        accounts.account_name,
        campaigns.campaign_id,
        campaigns.campaign_name,
        ad_sets.ad_set_id,
        ad_sets.ad_set_name,
        ads.ad_id,
        ads.ad_name,
        creatives.creative_id,
        creatives.creative_name,
        creatives.base_url,
        creatives.url_host,
        creatives.url_path,
        creatives.utm_source,
        creatives.utm_medium,
        creatives.utm_campaign,
        creatives.utm_content,
        creatives.utm_term,
        sum(report.clicks) as clicks,
        sum(report.impressions) as impressions,
        sum(report.spend) as spend
    from report
    left join ads 
        on cast(report.ad_id as 
    bigint
) = cast(ads.ad_id as 
    bigint
)
    left join creatives
        on cast(ads.creative_id as 
    bigint
) = cast(creatives.creative_id as 
    bigint
)
    left join ad_sets
        on cast(ads.ad_set_id as 
    bigint
) = cast(ad_sets.ad_set_id as 
    bigint
)
    left join campaigns
        on cast(ads.campaign_id as 
    bigint
) = cast(campaigns.campaign_id as 
    bigint
)
    left join accounts
        on cast(report.account_id as 
    bigint
) = cast(accounts.account_id as 
    bigint
)
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19


)

select *
from joined
      );
    