
  create or replace  view DATAFLOTEST_DATABASE.dbt_salesdataflo.VW_Employee  as (
    SELECT * FROM DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Employee
  );
