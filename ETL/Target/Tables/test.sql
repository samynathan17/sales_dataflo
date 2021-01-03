{% set results = dbt_utils.get_column_values(ref('Dim_Entity'),'entity_name') %}

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

{% for V_SF_Schema in results %}
{% if not loop.first %}
            UNION ALL
        {% endif %}
  select
        {{ dbt_utils.surrogate_key('id') }}  AS Account_ID,
        NAME AS Account_Name,
        TYPE AS Account_Type,
        ID AS Source_ID,
        IS_DELETED AS Active_Flag,
        --ACCOUNT_NUMBER AS ACCOUNT_NUMBER,
        INDUSTRY AS INDUSTRY,
        ANNUAL_REVENUE AS ANNUAL_REVENUE,
        OWNER_ID AS Employee_ID,
        CREATED_DATE as INITIAL_CREATE_DT,
        '{{ V_SF_Schema }}' as Source_type,
        'D_ACCOUNT_DIM_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ V_SF_Schema }}.Account
{% endfor %}