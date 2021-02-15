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
        {{ dbt_utils.surrogate_key('ID') }}  AS Audit_ID,
        ID as Source_id,
        MESSAGE,
        UPDATE_STARTED,
        UPDATE_ID,
        SCHEMA,
        --TABLE,
        --START,
        DONE,
        ROWS_UPDATED_OR_INSERTED,
        STATUS,
        PROGRESS,        
        '{{ V_SF_Schema }}' as Source_type,
        'D_FIVETRAN_AUDIT_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ V_SF_Schema }}.FIVETRAN_AUDIT
          {% if not loop.last %}
            UNION ALL
        {% endif %}        
    {% endif %}
{% endfor %}