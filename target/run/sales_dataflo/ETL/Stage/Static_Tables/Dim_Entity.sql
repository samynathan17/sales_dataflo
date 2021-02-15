

      create or replace transient table DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Entity  as
      (

select * from (
Select 1 entity_id,'SF_RKLIVE_06012021' entity_name, 'SF' entity_type,'D_ENTITY_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
union
Select 2 entity_id,'SF_TESTUSER_06012021' entity_name,'SF' entity_type,'D_ENTITY_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
)
      );
    