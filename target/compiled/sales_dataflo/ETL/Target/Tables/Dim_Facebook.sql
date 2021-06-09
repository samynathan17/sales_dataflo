

with base as (

    select *
    from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Ad

),fields as (

    select
        cast(date_day as date) as date_day,
        cast(campaign_id as 
    varchar
) as campaign_id,
        campaign_name,
        cast(ad_set_id as 
    varchar
) as ad_group_id,
        ad_set_name as ad_group_name,
        coalesce(clicks, 0) as clicks,
        coalesce(impressions, 0) as impressions,
        coalesce(spend, 0) as spend,
        coalesce(REACH, 0) as REACH,
        coalesce(CPC, 0) as CPC,
        coalesce(CPM, 0) as CPM,
        coalesce(CTR, 0) as CTR,
        coalesce(FREQUENCY, 0) as FREQUENCY,
        Source_type as platform
    from base
    
)

select *
from fields