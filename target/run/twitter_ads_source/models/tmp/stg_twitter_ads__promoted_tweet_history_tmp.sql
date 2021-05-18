
  create or replace  view DATAFLOTEST_DATABASE.dbt_salesdataflo.stg_twitter_ads__promoted_tweet_history_tmp  as (
    select *
from DATAFLOTEST_DATABASE.twitter_ads.promoted_tweet_history
  );
