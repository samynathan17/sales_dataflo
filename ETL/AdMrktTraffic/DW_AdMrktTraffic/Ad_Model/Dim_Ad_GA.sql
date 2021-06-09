with base as (

    select *
    from {{ ref('Dim_GA_Ads')}}

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
    {{ dbt_utils.group_by(8) }}

)

select *
from fields