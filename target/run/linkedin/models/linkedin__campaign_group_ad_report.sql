

      create or replace transient table DATAFLOTEST_DATABASE.dbt_salesdataflo.linkedin__campaign_group_ad_report  as
      (with adapter as (

    select *
    from DATAFLOTEST_DATABASE.dbt_salesdataflo.linkedin__ad_adapter

), grouped as (

    select 
        date_day,
        campaign_group_id,
        campaign_group_name,
        account_id,
        account_name,
        sum(cost) as cost,
        sum(clicks) as clicks, 
        sum(impressions) as impressions
    from adapter
    group by 1,2,3,4,5

)

select *
from grouped
      );
    