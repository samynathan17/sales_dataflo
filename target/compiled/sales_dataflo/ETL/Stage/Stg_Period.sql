







  
  select
        md5(cast(
    
    coalesce(cast(id as 
    varchar
), '')

 as 
    varchar
))  AS Period_ID,
        ID as Source_ID,
        FISCAL_YEAR_SETTINGS_ID,
        TYPE,
        START_DATE,
        END_DATE,
        SYSTEM_MODSTAMP,
        IS_FORECAST_PERIOD,
        QUARTER_LABEL,
        PERIOD_LABEL,
        NUMBER,
        FULLY_QUALIFIED_LABEL,
        'SF_TESTUSER_31122020' as Source_type,
        'D_ACCOUNT_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM SF_TESTUSER_31122020.Period
            
               UNION ALL
              
        


  
  select
        md5(cast(
    
    coalesce(cast(id as 
    varchar
), '')

 as 
    varchar
))  AS Period_ID,
        ID as Source_ID,
        FISCAL_YEAR_SETTINGS_ID,
        TYPE,
        START_DATE,
        END_DATE,
        SYSTEM_MODSTAMP,
        IS_FORECAST_PERIOD,
        QUARTER_LABEL,
        PERIOD_LABEL,
        NUMBER,
        FULLY_QUALIFIED_LABEL,
        'SF_RKLIVE_06012021' as Source_type,
        'D_ACCOUNT_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM SF_RKLIVE_06012021.Period
              
        
