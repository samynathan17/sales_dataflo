{% set results = get_column_values_from_query("select * from " ~ var('V_DB') ~ "." ~ var('V_Entity_Schema')~ "." ~ var('V_Sales')~" where DATASOURCE_TYPE = 'MC' and READY_TO_PROCESS = 'TRUE'", "ENTITY_DATASORUCE_NAME||'#'||DATASOURCE_TYPE")%}

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
        unique_key= 'Unsubscribe_ID',
        post_hook="DELETE FROM " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".STG_UNSUBSCRIBE WHERE Unsubscribe_ID IS NULL"
      )
}}


{% for V_SF_Schema in results %}

{% if V_SF_Schema != 'X' %} 
{% set schema_nm,entity_typ = V_SF_Schema.split('#') %}
{% else %}
{% set entity_typ = 'X' %}
{% endif %}

 {% if  entity_typ == 'MC'  %}   
      
  select
        {{ dbt_utils.surrogate_key('MEMBER_ID') }}  AS Unsubscribe_ID,
        MEMBER_ID,
        CAMPAIGN_ID,
        LIST_ID,
        TIMESTAMP,
        REASON,
        _FIVETRAN_SYNCED,
        '{{ schema_nm }}' as Source_type,
        'D_UNSUBSCRIBE_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ schema_nm }}.UNSUBSCRIBE
         {% if not loop.last %}
            UNION ALL
        {% endif %}
    {% elif  V_SF_Schema[0:1] == 'X'  %}     
       select
        null as Campaign_Recpt_ID,
        null AS CAMPAIGN_ID,
        null AS LIST_ID,
        null AS TIMESTAMP,
        null AS REASON,
        null AS _FIVETRAN_SYNCED,
        null as Source_type,
        null AS DW_SESSION_NM,   
    FROM dual    
    {% endif %}
{% endfor %}