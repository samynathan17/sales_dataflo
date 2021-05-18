with base as (

    select *
    from {{ ref('Stg_Final_URL_Performance')}}

), fields as (

    select
        cast(DATE as date) as date_day,
        account_name,
        external_customer_id as account_id,
        campaign_name,
        cast(campaign_id as {{ dbt_utils.type_string() }}) as campaign_id,
        ad_group_name,
        cast(ad_group_id as {{ dbt_utils.type_string() }}) as ad_group_id,
        coalesce(clicks, 0) as clicks,
        coalesce(impressions, 0) as impressions,
        coalesce(cost, 0) as spend,
        Source_Type as Platform
    from base

)

select *
from fields

