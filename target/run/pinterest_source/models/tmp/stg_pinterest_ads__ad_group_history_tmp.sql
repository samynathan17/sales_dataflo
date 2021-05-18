
  create or replace  view DATAFLOTEST_DATABASE.dbt_salesdataflo.stg_pinterest_ads__ad_group_history_tmp  as (
    select *
from DATAFLOTEST_DATABASE.pinterest_ads.ad_group_history
  );
