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
        unique_key= 'Stage_id'
      )
}}

{% for V_SF_Schema in results %}
{% if  V_SF_Schema[0:2] == 'SF'  %}   
 
  select
        {{ dbt_utils.surrogate_key('id') }}  AS Stage_id,
        ID as Source_ID,
        MASTER_LABEL,
        API_NAME,
        IS_ACTIVE,
        SORT_ORDER,
        IS_CLOSED,
        IS_WON,
        FORECAST_CATEGORY,
        FORECAST_CATEGORY_NAME,
        DEFAULT_PROBABILITY,
        DESCRIPTION,
        CREATED_BY_ID,
        CREATED_DATE,
        LAST_MODIFIED_BY_ID,
        LAST_MODIFIED_DATE,
        '{{ V_SF_Schema }}' as Source_type,
        'D_OPPORTUNITYSTAGES_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ V_SF_Schema }}.opportunity_stage 
           {% if not loop.last %}
            UNION ALL
           {% endif %}  
       {% endif %}
{% endfor %}