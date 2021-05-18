
  create or replace  view DATAFLOTEST_DATABASE.dbt_salesdataflo.stg_pinterest_ads__campaign_history_tmp  as (
    select *
from DATAFLOTEST_DATABASE.pinterest_ads.campaign_history
  );
