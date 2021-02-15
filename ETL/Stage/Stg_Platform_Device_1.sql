{% set results = get_column_values_from_query("select * from " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".DIM_MKT_ENTITY where ENTITY_TYPE = 'GA'", 'ENTITY_NAME')%}

{% if not var("enable_SF_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}


{% for V_SF_Schema in results %}

 {% if  V_SF_Schema[0:2] == 'GA'  %} 
      
  select
        {{ dbt_utils.surrogate_key('_FIVETRAN_ID') }}  AS ID,
        DATE,
        PROFILE,
        MOBILE_DEVICE_BRANDING,
        BROWSER,
        OPERATING_SYSTEM,
        GOAL_VALUE_ALL,
        GOAL_COMPLETIONS_ALL,
        GOAL_STARTS_ALL,
        GOAL_CONVERSION_RATE_ALL,
        GOAL_ABANDON_RATE_ALL,
        GOAL_ABANDONS_ALL,
        GOAL_VALUE_PER_SESSION,

        '{{ V_SF_Schema }}' as Source_type,
        'D_PLATFORM_DEVICE_1_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ V_SF_Schema }}.PLATFORM_DEVICE_1
          {% if not loop.last %}
            UNION ALL
        {% endif %}        
    {% endif %}
{% endfor %}