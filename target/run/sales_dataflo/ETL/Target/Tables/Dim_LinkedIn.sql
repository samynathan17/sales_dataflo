
  create or replace  view DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_LinkedIn  as (
    

with base as (

    select *
    from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Ad_LI

), fields as (

    select
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
        coalesce(clicks, 0) as clicks,
        coalesce(impressions, 0) as impressions,
        coalesce(cost, 0) as spend,
        Source_type as platform
    from base


)

select *
from fields
  );
