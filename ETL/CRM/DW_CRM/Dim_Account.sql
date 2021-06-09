{% if not var("enable_SF_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

{{
    config(
        materialized='incremental',
        unique_key= 'Account_ID'
      )
}}

WITH source AS
 (
 select * from {{ ref('Stg_Account') }}
  ),
DIM_ACCOUNT as (
      select
        Account_ID,
        NAME Account_Name,
        TYPE Account_Type,
        Source_ID,
        IS_DELETED Active_Flag,
        --ACCOUNT_NUMBER AS ACCOUNT_NUMBER,
        INDUSTRY INDUSTRY,
        ANNUAL_REVENUE AS ANNUAL_REVENUE,
        OWNER_ID AS Employee_ID,
        CREATED_DATE as INITIAL_CREATE_DT,
        Source_type Source_type,
        'D_ACCOUNT_DIM_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM source
    )
select * from DIM_ACCOUNT

