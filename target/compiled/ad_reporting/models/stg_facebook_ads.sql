

with base as (

    select *
    from DATAFLOTEST_DATABASE.dbt_salesdataflo.facebook_ads__ad_adapter

), fields as (

    select
        cast(date_day as date) as date_day,
        base_url,
        url_host,
        url_path,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        utm_term,
        cast(campaign_id as 
    varchar
) as campaign_id,
        campaign_name,
        cast(ad_set_id as 
    varchar
) as ad_group_id,
        ad_set_name as ad_group_name,
        'Facebook Ads' as platform,
        coalesce(clicks, 0) as clicks,
        coalesce(impressions, 0) as impressions,
        coalesce(spend, 0) as spend
    from base


)

select *
from fields