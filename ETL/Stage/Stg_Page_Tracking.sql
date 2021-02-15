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
        PAGE_TITLE,
        LANDING_PAGE_PATH,
        PAGE_PATH,
        EXIT_PAGE_PATH,
        PAGE_VALUE,
        EXIT_RATE,
        TIME_ON_PAGE,
        PAGEVIEWS_PER_SESSION,
        UNIQUE_PAGEVIEWS,
        ENTRANCE_RATE,

        
        '{{ V_SF_Schema }}' as Source_type,
        'D_PAGE_TRACKING_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ V_SF_Schema }}.PAGE_TRACKING
          {% if not loop.last %}
            UNION ALL
        {% endif %}        
    {% endif %}
{% endfor %}