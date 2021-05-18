

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
union
Select 9 metric_category_id,'Awareness' metrics_category,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
union
Select 10 metric_category_id,'Basic' metrics_category,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
union
Select 11 metric_category_id,'Engagement' metrics_category,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
union
Select 12 metric_category_id,'Analysis' metrics_category,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
union
Select 13 metric_category_id,'Cost' metrics_category,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
union
Select 14 metric_category_id,'Conversions' metrics_category,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
)