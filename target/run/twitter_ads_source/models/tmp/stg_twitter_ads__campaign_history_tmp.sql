
  create or replace  view DATAFLOTEST_DATABASE.dbt_salesdataflo.stg_twitter_ads__campaign_history_tmp  as (
    select *
from DATAFLOTEST_DATABASE.twitter_ads.campaign_history
  );
