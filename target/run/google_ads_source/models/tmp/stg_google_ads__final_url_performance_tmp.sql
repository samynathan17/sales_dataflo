
  create or replace  view DATAFLOTEST_DATABASE.dbt_salesdataflo.stg_google_ads__final_url_performance_tmp  as (
    select *
from DATAFLOTEST_DATABASE.adwords.final_url_performance
  );
