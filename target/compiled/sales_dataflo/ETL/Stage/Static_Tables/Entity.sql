select * from (
Select 1 entity_id,'SF_RKLIVE_06012021' entity_name, 'SF' entity_type,'D_ENTITY_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
union
Select 2 entity_id,'SF_TESTUSER_31122020' entity_name,'SF' entity_type,'D_ENTITY_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
)