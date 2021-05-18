with base as (

    select *
    from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_GA_Ads

), fields as (

    select
        date_day as date_day,
        platform,
        account_name,
        account_id,
        campaign_name,
        campaign_id,
        ad_group_name,
        ad_group_id,
        sum(spend) as spend,
        sum(clicks) as clicks,
        sum(impressions) as impressions
    from base
    group by 1,2,3,4,5,6,7,8

)

select *
from fields