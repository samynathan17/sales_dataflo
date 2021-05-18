
  create or replace  view DATAFLOTEST_DATABASE.dbt_salesdataflo.stg_pinterest_ads__pin_promotion_history_tmp  as (
    select *
from DATAFLOTEST_DATABASE.pinterest_ads.pin_promotion_history
  );
