
  create or replace  view DATAFLOTEST_DATABASE.dbt_salesdataflo.stg_twitter_ads__account_history_tmp  as (
    select *
from DATAFLOTEST_DATABASE.twitter_ads.account_history
  );
