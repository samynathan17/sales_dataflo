
  create or replace  view DATAFLOTEST_DATABASE.dbt_salesdataflo.VW_Contact  as (
    SELECT * FROM DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Contact
  );
