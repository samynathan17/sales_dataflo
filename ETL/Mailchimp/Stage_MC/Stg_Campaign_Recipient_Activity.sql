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
        unique_key= 'Campaign_Recpt_Act_ID',
        post_hook="DELETE FROM " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".STG_CAMPAIGN_RECIPIENT WHERE Campaign_Recpt_Act_ID IS NULL"
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
        {{ dbt_utils.surrogate_key('CAMPAIGN_ID') }}  AS Campaign_Recpt_Act_ID,
        ACTION,
        CAMPAIGN_ID,
        MEMBER_ID,
        TIMESTAMP,
        IP,
        URL,
        LIST_ID,
        COMBINATION_ID,
        BOUNCE_TYPE,
        _FIVETRAN_SYNCED,
        '{{ schema_nm }}' as Source_type,
        'D_CAMPAIGN_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ schema_nm }}.CAMPAIGN_RECIPIENT_ACTIVITY
         {% if not loop.last %}
            UNION ALL
        {% endif %}
    {% elif  V_SF_Schema[0:1] == 'X'  %}     
       select
        null as Campaign_Recpt_Act_ID,
        null AS ACTION,
        null as CAMPAIGN_ID,
        null as MEMBER_ID,
        null as TIMESTAMP,
        null as IP,
        null as URL,
        null as LIST_ID,
        null as COMBINATION_ID,
        null as BOUNCE_TYPE,
        null as _FIVETRAN_SYNCED,
        null as Source_type,
        null AS DW_SESSION_NM,   
    FROM dual    
    {% endif %}
{% endfor %}