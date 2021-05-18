
  create or replace  view DATAFLOTEST_DATABASE.dbt_salesdataflo.stg_facebook_ads__ad_set_history_tmp  as (
    select * from DATAFLOTEST_DATABASE.FB_ADS_DRGRILL_30032021.ad_set_history
  );
