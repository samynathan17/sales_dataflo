
  create or replace  view DATAFLOTEST_DATABASE.dbt_salesdataflo.stg_twitter_ads__line_item_history_tmp  as (
    select *
from DATAFLOTEST_DATABASE.twitter_ads.line_item_history
  );
