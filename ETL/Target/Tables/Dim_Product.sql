 {% if not var("enable_SF_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}
/*
{{
    config(
        materialized='incremental',
        unique_key= 'product_id'
      )
}}


    WITH source AS (
        SELECT * from {{ var('V_SF_Schema') }}.product
    ),Dim_Product as(
        SELECT
            NULL AS account_id,
            {{ dbt_utils.surrogate_key('id') }} AS product_id,
            ID AS Source_id,
            PRODUCT_CODE AS product_code,
            NAME AS product_name,
            IS_ACTIVE AS active_flag,
            QUANTITY_UNIT_OF_MEASURE AS Quantity_UOM,
             {% if var("V_SF_CRM_ETL") == 'FIVETRAN_SF' %}  'SF' {% endif %} as Source_type,
            'D_PRODUCT_DIM_LOAD'  AS DW_SESSION_NM,
            {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
         FROM
            source 
     
    )    
select * from Dim_Product
*/