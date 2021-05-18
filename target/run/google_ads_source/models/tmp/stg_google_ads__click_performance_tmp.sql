
  create or replace  view DATAFLOTEST_DATABASE.dbt_salesdataflo.stg_google_ads__click_performance_tmp  as (
    select *
from DATAFLOTEST_DATABASE.adwords.click_performance
  );
