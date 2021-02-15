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
        unique_key= 'OWNER_ID'
      )
}}

{% for V_SF_Schema in results %}
 {% if  V_SF_Schema[0:2] == 'HS'  %}    
  select
        {{ dbt_utils.surrogate_key('OWNER_ID') }}  AS OWNER_ID,
        OWNER_ID as Source_OWNER_ID,
        PORTAL_ID,
        TYPE,
        FIRST_NAME,
        LAST_NAME,
        EMAIL,
        CREATED_AT,
        UPDATED_AT,
        IS_ACTIVE,
        ACTIVE_USER_ID,
        USER_ID_INCLUDING_INACTIVE,   
        '{{ V_SF_Schema }}' as Source_type,
        'D_OWNER_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS    
    FROM {{ V_SF_Schema }}.Owner
    {% if not loop.last %}
            UNION ALL
        {% endif %}
    {% endif %}
{% endfor %}

