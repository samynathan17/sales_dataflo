
  create or replace  view DATAFLOTEST_DATABASE.dbt_salesdataflo.VW_Opportunity  as (
    SELECT * FROM DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Opportunity
  );
