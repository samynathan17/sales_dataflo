







  select
        md5(cast(
    
    coalesce(cast(id as 
    varchar
), '')

 as 
    varchar
))  AS Account_ID,
        NAME AS Account_Name,
        TYPE AS Account_Type,
        ID AS Source_ID,
        IS_DELETED AS Active_Flag,
        --ACCOUNT_NUMBER AS ACCOUNT_NUMBER,
        INDUSTRY AS INDUSTRY,
        ANNUAL_REVENUE AS ANNUAL_REVENUE,
        OWNER_ID AS Employee_ID,
        CREATED_DATE as INITIAL_CREATE_DT,
        'DBT_TEST_LIVEDATA_RK' as Source_type,
        'D_ACCOUNT_DIM_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM DBT_TEST_LIVEDATA_RK.Account


            UNION ALL
        
  select
        md5(cast(
    
    coalesce(cast(id as 
    varchar
), '')

 as 
    varchar
))  AS Account_ID,
        NAME AS Account_Name,
        TYPE AS Account_Type,
        ID AS Source_ID,
        IS_DELETED AS Active_Flag,
        --ACCOUNT_NUMBER AS ACCOUNT_NUMBER,
        INDUSTRY AS INDUSTRY,
        ANNUAL_REVENUE AS ANNUAL_REVENUE,
        OWNER_ID AS Employee_ID,
        CREATED_DATE as INITIAL_CREATE_DT,
        'SALESFORCE_FREETRAILS' as Source_type,
        'D_ACCOUNT_DIM_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM SALESFORCE_FREETRAILS.Account
