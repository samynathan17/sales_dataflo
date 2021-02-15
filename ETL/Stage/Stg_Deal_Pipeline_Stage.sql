{% set results = get_column_values_from_query("select * from " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".DIM_SALES_ENTITY where ENTITY_TYPE = 'HS'", 'ENTITY_NAME')%}

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
        unique_key= 'PIPELINE_STAGE_ID'
      )
}}

{% for V_SF_Schema in results %}
 {% if  V_SF_Schema[0:2] == 'HS'  %}    
  select
        {{ dbt_utils.surrogate_key('STAGE_ID') }}  AS PIPELINE_STAGE_ID,
        STAGE_ID as Source_STAGE_ID,
        PIPELINE_ID,
        LABEL,
        PROBABILITY,
        ACTIVE,
        DISPLAY_ORDER,
        CLOSED_WON, 
        '{{ V_SF_Schema }}' as Source_type,
        'D_DEAL_PIPELINE_STAGE_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS    
    FROM {{ V_SF_Schema }}.DEAL_PIPELINE_STAGE
    {% if not loop.last %}
            UNION ALL
        {% endif %}
    {% endif %}
{% endfor %}

