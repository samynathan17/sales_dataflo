{{ config(
    materialized="table"
) 
}}

select *from (
Select 1 metric_category_id,'Productivity' metrics_category,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
union
Select 2 metric_category_id,'Opportunity Generation' metrics_category,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
union
Select 3 metric_category_id,'Lead Generation' metrics_category,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
union
Select 4 metric_category_id,'Conversion' metrics_category,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
union
Select 5 metric_category_id,'Account' metrics_category,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
union
Select 6 metric_category_id,'Contact' metrics_category,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
union
Select 7 metric_category_id,'Sub-list' metrics_category,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
union
Select 8 metric_category_id,'Funnel' metrics_category,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
)