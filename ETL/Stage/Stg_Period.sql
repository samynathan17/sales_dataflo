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
        unique_key= 'Period_ID'
      )
}}

{% for V_SF_Schema in results %}
{% if  V_SF_Schema[0:2] == 'SF'  %}
  
  select
        {{ dbt_utils.surrogate_key('id') }}  AS Period_ID,
        ID as Source_ID,
        FISCAL_YEAR_SETTINGS_ID,
        TYPE,
        START_DATE,
        END_DATE,
        SYSTEM_MODSTAMP,
        IS_FORECAST_PERIOD,
        QUARTER_LABEL,
        PERIOD_LABEL,
        NUMBER,
        FULLY_QUALIFIED_LABEL,
        '{{ V_SF_Schema }}' as Source_type,
        'D_ACCOUNT_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ V_SF_Schema }}.Period
            {% if not loop.last %}
               UNION ALL
            {% endif %}  
        {% endif %}
{% endfor %}