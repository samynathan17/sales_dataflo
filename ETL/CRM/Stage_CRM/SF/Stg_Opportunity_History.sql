{% set results = get_column_values_from_query("select * from " ~ var('V_DB') ~ "." ~ var('V_Entity_Schema')~ "." ~ var('V_Sales')~" where DATASOURCE_TYPE = 'SF' and READY_TO_PROCESS = 'TRUE'", "ENTITY_DATASORUCE_NAME||'#'||DATASOURCE_TYPE")%}

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
        unique_key= 'Opportunity_Calc_id',
        post_hook="DELETE FROM " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".STG_OPPORTUNITY_HISTORY WHERE Opportunity_Calc_id IS NULL"
      )
}}

{% for V_SF_Schema in results %}

{% if V_SF_Schema != 'X' %} 
{% set schema_nm,entity_typ = V_SF_Schema.split('#') %}
{% else %}
{% set entity_typ = 'X' %}
{% endif %}

 {% if  entity_typ == 'SF'  %}   
 
  select
        {{ dbt_utils.surrogate_key('id') }}  AS Opportunity_Calc_id,
        ID as Source_ID,        
        OPPORTUNITY_ID,
        CREATED_BY_ID,
        CREATED_DATE,
        STAGE_NAME,
        AMOUNT,
        EXPECTED_REVENUE,
        CLOSE_DATE,
        PROBABILITY,
        FORECAST_CATEGORY,
        SYSTEM_MODSTAMP,
        IS_DELETED,
        PREV_AMOUNT,
        PREV_CLOSE_DATE,
         null AS CUSTOMER_TEXT_1,
        NULL AS CUSTOMER_TEXT_2,
        NULL AS CUSTOMER_TEXT_3,
        NULL AS CUSTOMER_TEXT_4,
        NULL AS CUSTOMER_TEXT_5,
        NULL AS CUSTOMER_TEXT_6,
        NULL AS CUSTOMER_NUMBER_1,
        NULL AS CUSTOMER_NUMBER_2,
        NULL AS CUSTOMER_NUMBER_3,
        NULL AS CUSTOMER_DATE_1,
        NULL AS CUSTOMER_DATE_2,
        NULL AS CUSTOMER_DATE_3,
        '{{ schema_nm }}' as Source_type,
        'D_OPPORTUNITY_HISTORY_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ schema_nm }}.OPPORTUNITY_HISTORY 
           {% if not loop.last %}
            UNION ALL
           {% endif %}  
{% elif  entity_typ == 'X'  %}     
       select
        null as Stage_id,
        null as  Source_ID,
        null as MASTER_LABEL,
        null as API_NAME,
        null as IS_ACTIVE,
        null as SORT_ORDER,
        null as IS_CLOSED,
        null as IS_WON,
        null as FORECAST_CATEGORY,
        null as FORECAST_CATEGORY_NAME,
        null as DEFAULT_PROBABILITY,
        null as DESCRIPTION,
        null as CREATED_BY_ID,
        null as CREATED_DATE,
        null as LAST_MODIFIED_BY_ID,
        null as LAST_MODIFIED_DATE,
        null as Source_type,
        null as DW_SESSION_NM,
        null as DW_INS_UPD_DTS,
         null AS CUSTOMER_TEXT_1,
        NULL AS CUSTOMER_TEXT_2,
        NULL AS CUSTOMER_TEXT_3,
        NULL AS CUSTOMER_TEXT_4,
        NULL AS CUSTOMER_TEXT_5,
        NULL AS CUSTOMER_TEXT_6,
        NULL AS CUSTOMER_NUMBER_1,
        NULL AS CUSTOMER_NUMBER_2,
        NULL AS CUSTOMER_NUMBER_3,
        NULL AS CUSTOMER_DATE_1,
        NULL AS CUSTOMER_DATE_2,
        NULL AS CUSTOMER_DATE_3   
    FROM dual    
    {% endif %}
{% endfor %}