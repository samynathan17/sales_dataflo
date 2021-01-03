

      create or replace transient table DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Entity  as
      (

select * from (
Select 1 entity_id,'DBT_TEST_LIVEDATA_RK' entity_name,'D_ENTITY_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
union
Select 2 entity_id,'SALESFORCE_FREETRAILS' entity_name,'D_ENTITY_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
)
      );
    