

      create or replace transient table DATAFLOTEST_DATABASE.dbt_salesdataflo.stg_microsoft_ads__ad_performance_daily_report  as
      (with base as (

    select *
    from DATAFLOTEST_DATABASE.bingads.ad_performance_daily_report

), fields as (

    select 
        date as date_day,
        account_id,
        campaign_id,
        ad_group_id,
        ad_id,
        currency_code,
        clicks,
        impressions,
        spend
    from base

)

select *
from fields
      );
    