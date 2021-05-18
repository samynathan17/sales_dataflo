
  create or replace  view DATAFLOTEST_DATABASE.dbt_salesdataflo.stg_linkedin_ads  as (
    

with base as (

    select *
    from DATAFLOTEST_DATABASE.dbt_salesdataflo.linkedin__ad_adapter

), fields as (

    select
        'LinkedIn Ads' as platform,
        cast(date_day as date) as date_day,
        account_name,
        account_id,
        campaign_name,
        cast(campaign_id as 
    varchar
) as campaign_id,
        campaign_group_name as ad_group_name,
        cast(campaign_group_id as 
    varchar
) as ad_group_id,
        base_url,
        url_host,
        url_path,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        utm_term,
        coalesce(clicks, 0) as clicks,
        coalesce(impressions, 0) as impressions,
        coalesce(cost, 0) as spend
    from base


)

select *
from fields
  );
