{% set results = get_column_values_from_query("select * from " ~ var('V_DB') ~ "." ~ var('V_Entity_Schema') ~ "." ~ var('V_Sales')~" e left outer join INFORMATION_SCHEMA.COLUMNS a on e.ENTITY_DATASORUCE_NAME = a.TABLE_SCHEMA and column_name = 'CURRENCY_ISO_CODE' and TABLE_NAME ='OPPORTUNITY' where DATASOURCE_TYPE = 'SF' and READY_TO_PROCESS = 'TRUE'", "ENTITY_DATASORUCE_NAME ||'#'|| DATASOURCE_TYPE ||'#'|| nvl(COLUMN_NAME,'NA') ||'#'||REPORTING_CURRENCY") %}

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
        unique_key= 'opportunity_id',
        post_hook="DELETE FROM " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".STG_OPPORTUNITY WHERE opportunity_id IS NULL"
      )
}}

{% for V_SF_Schema in results %}

{% if V_SF_Schema != 'X' %} 
{% set entity_name, entity_typ, col, REPORTING_CURRENCY = V_SF_Schema.split('#') %}
{% else %}
{% set entity_typ = 'X' %}
{% endif %}

{% if  entity_typ == 'SF'  %}    
  
  select
        {{ dbt_utils.surrogate_key('id') }}  AS opportunity_id,
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
        {% if  col == 'NA'  %} 'USD' {% else %} CURRENCY_ISO_CODE {% endif %} as CURRENCY_ISO_CODE,
        '{{ REPORTING_CURRENCY }}' as REPORTING_CURRENCY,
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
        '{{ entity_name }}' as Source_type,
        'D_OPPORTUNITY_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ entity_name }}.opportunity
        {% if not loop.last %}
            UNION ALL
        {% endif %}  
 {% elif  entity_typ == 'X'  %}     
       select
        null as opportunity_id,
        null as  Source_ID,
        null as IS_DELETED,
        null as ACCOUNT_ID,
        null as NAME,
        null as DESCRIPTION,
        null as STAGE_NAME,
        null as AMOUNT,
        null as CLOSE_DATE,
        null as TYPE,
        null as NEXT_STEP,
        null as LEAD_SOURCE,
        null as IS_CLOSED,
        null as IS_WON,
        null as CURRENCY_ISO_CODE,
        null as REPORTING_CURRENCY,
        null as FORECAST_CATEGORY,
        null as FORECAST_CATEGORY_NAME,
        null as HAS_OPPORTUNITY_LINE_ITEM,
        null as OWNER_ID,
        null as CREATED_DATE,
        null as CREATED_BY_ID,
        null as LAST_MODIFIED_DATE,
        null as LAST_MODIFIED_BY_ID,
        null as SYSTEM_MODSTAMP,
        null as LAST_ACTIVITY_DATE,
        null as FISCAL_QUARTER,
        null as FISCAL_YEAR,
        null as FISCAL,
        null as LAST_VIEWED_DATE,
        null as LAST_REFERENCED_DATE,
        null as HAS_OPEN_ACTIVITY,
        null as HAS_OVERDUE_TASK,
        null as CONTACT_ID,
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