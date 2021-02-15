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
        unique_key= 'opportunity_id'
      )
}}

{% for V_SF_Schema in results %}

{% if  V_SF_Schema[0:2] == 'SF'  %}    
  
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
        '{{ V_SF_Schema }}' as Source_type,
        'D_OPPORTUNITY_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ V_SF_Schema }}.opportunity
        {% if not loop.last %}
            UNION ALL
        {% endif %}  
    {% endif %}
{% endfor %}