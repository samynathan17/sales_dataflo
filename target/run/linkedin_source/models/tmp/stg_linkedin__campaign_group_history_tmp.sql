
  create or replace  view DATAFLOTEST_DATABASE.dbt_salesdataflo.stg_linkedin__campaign_group_history_tmp  as (
    select *
from DATAFLOTEST_DATABASE.LINKEDIN_ADS_19032021.campaign_group_history
  );
