{% set results = get_column_values_from_query("select * from " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".DIM_SALES_ENTITY where ENTITY_TYPE = 'SF'", 'ENTITY_NAME')%}

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

 {% if  V_SF_Schema[0:2] == 'SF'  %}   
      
  select
        {{ dbt_utils.surrogate_key('id') }}  AS Account_ID,
        ID as Source_ID,
        IS_DELETED,
        MASTER_RECORD_ID,
        NAME,
        TYPE,
        PARENT_ID,
        BILLING_STREET,
        BILLING_CITY,
        BILLING_STATE,
        BILLING_POSTAL_CODE,
        BILLING_COUNTRY,
        SHIPPING_STREET,
        SHIPPING_CITY,
        SHIPPING_STATE,
        SHIPPING_POSTAL_CODE,
        SHIPPING_COUNTRY,
        PHONE,
        FAX,
        WEBSITE,
        SIC,
        INDUSTRY,
        ANNUAL_REVENUE,
        NUMBER_OF_EMPLOYEES,
        OWNERSHIP,
        DESCRIPTION,
        OWNER_ID,
        CREATED_DATE,
        CREATED_BY_ID,
        LAST_MODIFIED_DATE,
        LAST_MODIFIED_BY_ID,
        ACCOUNT_SOURCE,
        SIC_DESC,
        '{{ V_SF_Schema }}' as Source_type,
        'D_ACCOUNT_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ V_SF_Schema }}.Account
         {% if not loop.last %}
            UNION ALL
        {% endif %}
    {% endif %}
{% endfor %}