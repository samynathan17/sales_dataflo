



WITH source AS
 (
 select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Account
  ),
DIM_ACCOUNT as (
      select
        Account_ID,
        NAME AS Account_Name,
        TYPE AS Account_Type,
        Source_ID,
        IS_DELETED AS Active_Flag,
        --ACCOUNT_NUMBER AS ACCOUNT_NUMBER,
        INDUSTRY AS INDUSTRY,
        ANNUAL_REVENUE AS ANNUAL_REVENUE,
        OWNER_ID AS Employee_ID,
        CREATED_DATE as INITIAL_CREATE_DT,
        Source_type,
        'D_ACCOUNT_DIM_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM source
    )
select * from DIM_ACCOUNT