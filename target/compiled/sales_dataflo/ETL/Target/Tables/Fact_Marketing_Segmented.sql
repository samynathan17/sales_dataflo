-- depends_on: DATAFLOTEST_DATABASE.dbt_salesdataflo.Temp_GA_segmented
-- depends_on: DATAFLOTEST_DATABASE.dbt_salesdataflo.Temp_GSC_Segmented


select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Temp_GA_segmented
union all
select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Temp_GSC_Segmented