

      create or replace transient table DATAFLOTEST_DATABASE.dbt_salesdataflo.facebook_ads__account_report  as
      (with adapter as (

    select *
    from DATAFLOTEST_DATABASE.dbt_salesdataflo.facebook_ads__ad_adapter

), aggregated as (

    select
        date_day,
        account_id,
        account_name,
        sum(clicks) as clicks,
        sum(impressions) as impressions,
        sum(spend) as spend
    from adapter
    group by 1,2,3

)

select *
from aggregated
      );
    