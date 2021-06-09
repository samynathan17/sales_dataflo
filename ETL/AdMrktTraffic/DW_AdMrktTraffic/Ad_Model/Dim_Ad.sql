with report as (

    select *
    from {{ ref('Stg_Basic_Ad') }}

), creatives as (

    select *
    from {{ ref('Dim_Creative_History_FB') }}

), accounts as (

    select *
    from {{ ref('Dim_Account_History_FB') }}
    where is_most_recent_record = true

), ads as (

    select *
    from {{ ref('Dim_Ad_History') }}
    where is_most_recent_record = true

), ad_sets as (

    select *
    from {{ ref('Dim_Ad_Set_History') }}
    where is_most_recent_record = true

), campaigns as (

    select *
    from {{ ref('Dim_Campaign_History_FB') }}
    where is_most_recent_record = true

), joined as (

    select
        report.DATE as date_day,
        report.Source_type,
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
        sum(report.INLINE_LINK_CLICKS) as clicks,
        sum(report.impressions) as impressions,
        sum(report.spend) as spend,
        sum(report.REACH) as REACH,
        sum(report.CPC) as CPC,
        sum(report.CPM) as CPM,
        sum(report.CTR) as CTR,
        sum(report.FREQUENCY) as FREQUENCY
    from report
    left join ads 
        on cast(report.ad_id as {{ dbt_utils.type_bigint() }}) = cast(ads.ad_id as {{ dbt_utils.type_bigint() }})
    left join creatives
        on cast(ads.creative_id as {{ dbt_utils.type_bigint() }}) = cast(creatives.creative_id as {{ dbt_utils.type_bigint() }})
    left join ad_sets
        on cast(ads.ad_set_id as {{ dbt_utils.type_bigint() }}) = cast(ad_sets.ad_set_id as {{ dbt_utils.type_bigint() }})
    left join campaigns
        on cast(ads.campaign_id as {{ dbt_utils.type_bigint() }}) = cast(campaigns.campaign_id as {{ dbt_utils.type_bigint() }})
    left join accounts
        on cast(report.account_id as {{ dbt_utils.type_bigint() }}) = cast(accounts.account_id as {{ dbt_utils.type_bigint() }})
    {{ dbt_utils.group_by(12) }}
)

select *
from joined