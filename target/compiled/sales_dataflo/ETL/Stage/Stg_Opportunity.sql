







    
  
  select
        md5(cast(
    
    coalesce(cast(id as 
    varchar
), '')

 as 
    varchar
))  AS opportunity_id,
        ID as Source_ID,
        IS_DELETED,
        ACCOUNT_ID,
        NAME,
        DESCRIPTION,
        STAGE_NAME,
        AMOUNT,
        CLOSE_DATE,
        TYPE,
        NEXT_STEP,
        LEAD_SOURCE,
        IS_CLOSED,
        IS_WON,
        FORECAST_CATEGORY,
        FORECAST_CATEGORY_NAME,
        HAS_OPPORTUNITY_LINE_ITEM,
        OWNER_ID,
        CREATED_DATE,
        CREATED_BY_ID,
        LAST_MODIFIED_DATE,
        LAST_MODIFIED_BY_ID,
        SYSTEM_MODSTAMP,
        LAST_ACTIVITY_DATE,
        FISCAL_QUARTER,
        FISCAL_YEAR,
        FISCAL,
        LAST_VIEWED_DATE,
        LAST_REFERENCED_DATE,
        HAS_OPEN_ACTIVITY,
        HAS_OVERDUE_TASK,
        CONTACT_ID,
        'SF_TESTUSER_31122020' as Source_type,
        'D_OPPORTUNITY_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM SF_TESTUSER_31122020.opportunity
        
            UNION ALL
          
    


    
  
  select
        md5(cast(
    
    coalesce(cast(id as 
    varchar
), '')

 as 
    varchar
))  AS opportunity_id,
        ID as Source_ID,
        IS_DELETED,
        ACCOUNT_ID,
        NAME,
        DESCRIPTION,
        STAGE_NAME,
        AMOUNT,
        CLOSE_DATE,
        TYPE,
        NEXT_STEP,
        LEAD_SOURCE,
        IS_CLOSED,
        IS_WON,
        FORECAST_CATEGORY,
        FORECAST_CATEGORY_NAME,
        HAS_OPPORTUNITY_LINE_ITEM,
        OWNER_ID,
        CREATED_DATE,
        CREATED_BY_ID,
        LAST_MODIFIED_DATE,
        LAST_MODIFIED_BY_ID,
        SYSTEM_MODSTAMP,
        LAST_ACTIVITY_DATE,
        FISCAL_QUARTER,
        FISCAL_YEAR,
        FISCAL,
        LAST_VIEWED_DATE,
        LAST_REFERENCED_DATE,
        HAS_OPEN_ACTIVITY,
        HAS_OVERDUE_TASK,
        CONTACT_ID,
        'SF_RKLIVE_06012021' as Source_type,
        'D_OPPORTUNITY_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM SF_RKLIVE_06012021.opportunity
          
    
