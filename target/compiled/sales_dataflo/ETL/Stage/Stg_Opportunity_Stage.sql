






   
 
  select
        md5(cast(
    
    coalesce(cast(id as 
    varchar
), '')

 as 
    varchar
))  AS Stage_id,
        ID as Source_ID,
        MASTER_LABEL,
        API_NAME,
        IS_ACTIVE,
        SORT_ORDER,
        IS_CLOSED,
        IS_WON,
        FORECAST_CATEGORY,
        FORECAST_CATEGORY_NAME,
        DEFAULT_PROBABILITY,
        DESCRIPTION,
        CREATED_BY_ID,
        CREATED_DATE,
        LAST_MODIFIED_BY_ID,
        LAST_MODIFIED_DATE,
        'SF_TESTUSER_31122020' as Source_type,
        'D_OPPORTUNITYSTAGES_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM SF_TESTUSER_31122020.opportunity_stage 
           
            UNION ALL
             
       

   
 
  select
        md5(cast(
    
    coalesce(cast(id as 
    varchar
), '')

 as 
    varchar
))  AS Stage_id,
        ID as Source_ID,
        MASTER_LABEL,
        API_NAME,
        IS_ACTIVE,
        SORT_ORDER,
        IS_CLOSED,
        IS_WON,
        FORECAST_CATEGORY,
        FORECAST_CATEGORY_NAME,
        DEFAULT_PROBABILITY,
        DESCRIPTION,
        CREATED_BY_ID,
        CREATED_DATE,
        LAST_MODIFIED_BY_ID,
        LAST_MODIFIED_DATE,
        'SF_RKLIVE_06012021' as Source_type,
        'D_OPPORTUNITYSTAGES_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM SF_RKLIVE_06012021.opportunity_stage 
             
       
