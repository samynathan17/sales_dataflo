
  create or replace  view DATAFLOTEST_DATABASE.dbt_salesdataflo.stg_linkedin__creative_history_tmp  as (
    select *
from DATAFLOTEST_DATABASE.LINKEDIN_ADS_19032021.creative_history
  );
