-- depends_on: DATAFLOTEST_DATABASE.dbt_salesdataflo.Temp_GA_ADs
-- depends_on: DATAFLOTEST_DATABASE.dbt_salesdataflo.Temp_Facebook
-- depends_on: DATAFLOTEST_DATABASE.dbt_salesdataflo.Temp_Linkedin


select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Temp_GA_ADs
union all
select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Temp_Facebook
union all
select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Temp_Linkedin