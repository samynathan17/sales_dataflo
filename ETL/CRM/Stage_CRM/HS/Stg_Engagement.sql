{% set results = get_column_values_from_query("select * from " ~ var('V_DB') ~ "." ~ var('V_Entity_Schema')~ "." ~ var('V_Sales')~" where DATASOURCE_TYPE = 'HS'  and READY_TO_PROCESS = 'TRUE'", "ENTITY_DATASORUCE_NAME||'#'||DATASOURCE_TYPE")%}

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
        unique_key= 'Engagement_ID',
        post_hook="DELETE FROM " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".STG_ENGAGEMENT WHERE Engagement_ID IS NULL"
      )
}}

{% for V_SF_Schema in results %}

{% if V_SF_Schema != 'X' %} 
{% set schema_nm,entity_typ = V_SF_Schema.split('#') %}
{% else %}
{% set entity_typ = 'X' %}
{% endif %}

 {% if  entity_typ == 'HS'  %} 
 
  select
        {{ dbt_utils.surrogate_key('id') }}  AS Engagement_ID,
        ID as Source_ID,
        PORTAL_ID,
        ACTIVE,
        OWNER_ID,
        TYPE,
        ACTIVITY_TYPE,
        CREATED_AT,
        LAST_UPDATED,
        TIMESTAMP,
        _FIVETRAN_SYNCED,
        '{{ schema_nm }}' as Source_type,
        'D_COMPANY_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS    
    FROM {{ schema_nm }}.ENGAGEMENT
     {% if not loop.last %}
            UNION ALL
        {% endif %}
    {% elif  entity_typ == 'X'  %}     
       select
        null  AS Engagement_ID,
        null  AS Source_ID,
        null  AS PORTAL_ID,
        null  AS ACTIVE,
        null  AS OWNER_ID,
        null  AS TYPE,
        null  AS ACTIVITY_TYPE,
        null  AS CREATED_AT,
        null  AS LAST_UPDATED,
        null  AS TIMESTAMP,
        null  AS _FIVETRAN_SYNCED,
        '{{ schema_nm }}' as Source_type,
        'D_COMPANY_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM dual      
    {% endif %}
{% endfor %}

