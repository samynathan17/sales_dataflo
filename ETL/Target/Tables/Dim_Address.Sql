{% if not var("enable_SF_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

{{ config(
    materialized="table"
) 
}}

WITH source AS (
       select *  from {{ var('V_SF_Schema') }}.Contact 
    ),Dim_Address AS(
        SELECT 
            {{ dbt_utils.surrogate_key('id') }} AS  address_id,
            account_id AS account_id,
            id as source_id,
            MAILING_STREET AS STREET,
            MAILING_CITY AS CITY,
            MAILING_STATE AS STATE,
            MAILING_POSTAL_CODE AS POSTAL_CODE,
            MAILING_COUNTRY AS COUNTRY,
            'Billing' as address_type,
            {% if var("V_SF_CRM_ETL") == 'FIVETRAN_SF' %}  'SF' {% endif %} as Source_type,
             'D_ADDRESS_DIM_LOAD' AS DW_SESSION_NM,
            {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
            from source
        union 
        SELECT
            {{ dbt_utils.surrogate_key('id') }} AS  address_id,
            account_id AS account_id,
            id as source_id,
            OTHER_STREET AS STREET,
            OTHER_CITY AS CITY,
            OTHER_STATE AS STATE,
            OTHER_POSTAL_CODE AS POSTAL_CODE,
            OTHER_COUNTRY As COUNTRY, 
            'Shipping' as address_type,
            {% if var("V_SF_CRM_ETL") == 'FIVETRAN_SF' %}  'SF' {% endif %} as Source_type,
             'D_ADDRESS_DIM_LOAD' AS DW_SESSION_NM,
            {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
             from source
    )
    
select * from Dim_Address

