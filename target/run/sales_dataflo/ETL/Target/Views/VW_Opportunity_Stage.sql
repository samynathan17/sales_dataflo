
  create or replace  view DATAFLOTEST_DATABASE.dbt_salesdataflo.VW_Opportunity_Stage  as (
    SELECT * FROM DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Opportunity_Stage
  );
