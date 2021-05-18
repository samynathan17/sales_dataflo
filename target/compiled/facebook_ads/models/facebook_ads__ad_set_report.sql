with adapter as (

    select *
    from DATAFLOTEST_DATABASE.dbt_salesdataflo.facebook_ads__ad_adapter

), aggregated as (

    select
        date_day,
        account_id,
        account_name,
        campaign_id,
        campaign_name,
        ad_set_id,
        ad_set_name,
        sum(clicks) as clicks,
        sum(impressions) as impressions,
        sum(spend) as spend
    from adapter
    group by 1,2,3,4,5,6,7

)

select *
from aggregated