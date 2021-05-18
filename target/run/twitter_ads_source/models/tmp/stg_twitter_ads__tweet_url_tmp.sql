
  create or replace  view DATAFLOTEST_DATABASE.dbt_salesdataflo.stg_twitter_ads__tweet_url_tmp  as (
    select *
from DATAFLOTEST_DATABASE.twitter_ads.tweet_url
  );
