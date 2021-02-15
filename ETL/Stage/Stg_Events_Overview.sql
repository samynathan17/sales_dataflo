{% set results = get_column_values_from_query("select * from " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".DIM_MKT_ENTITY where ENTITY_TYPE = 'GA'", 'ENTITY_NAME')%}

{% if not var("enable_SF_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}


{% for V_SF_Schema in results %}

 {% if  V_SF_Schema == 'GA_DATAFLO_01022021'  %} 
    
  select
        {{ dbt_utils.surrogate_key('_FIVETRAN_ID') }}  AS ID,
        DATE,
        PROFILE,
        EVENT_CATEGORY,
        EVENT_VALUE,
        TOTAL_EVENTS,
        SESSIONS_WITH_EVENT,
        EVENTS_PER_SESSION_WITH_EVENT,
        AVG_EVENT_VALUE,
        UNIQUE_EVENTS,
        '{{ V_SF_Schema }}' as Source_type,
        'D_EVENTS_OVERVIEW_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ V_SF_Schema }}.EVENTS_OVERVIEW
        
    {% endif %}
{% endfor %}