
  create or replace  view DATAFLOTEST_DATABASE.dbt_salesdataflo.stg_linkedin__ad_analytics_by_creative_tmp  as (
    select *
from DATAFLOTEST_DATABASE.LINKEDIN_ADS_19032021.ad_analytics_by_creative
  );
