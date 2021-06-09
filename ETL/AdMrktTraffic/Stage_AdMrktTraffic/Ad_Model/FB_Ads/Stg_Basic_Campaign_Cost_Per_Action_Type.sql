{% set results = get_column_values_from_query("select * from " ~ var('V_DB') ~ "." ~ var('V_Entity_Schema')~ "." ~ var('V_Mkt')~" where DATASOURCE_TYPE = 'FB_ADS' and READY_TO_PROCESS = 'TRUE'", "ENTITY_DATASORUCE_NAME||'#'||DATASOURCE_TYPE")%}

{% if not var("enable_SF_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

{% for V_SF_Schema in results %}


{% if V_SF_Schema != 'X' %} 
{% set schema_nm,entity_typ = V_SF_Schema.split('#') %}
{% else %}
{% set entity_typ = 'X' %}
{% endif %}

 {% if  entity_typ == 'FB_ADS'  %} 
      
  select
        {{ dbt_utils.surrogate_key('CAMPAIGN_ID') }}  AS ID,
       CAMPAIGN_ID,
 _1_D_VIEW,
 DATE,
 _FIVETRAN_ID,
 INDEX,
 ACTION_TYPE,
 _7_D_CLICK,

        '{{ schema_nm }}' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ schema_nm }}.BASIC_CAMPAIGN_COST_PER_ACTION_TYPE
          {% if not loop.last %}
            UNION ALL
        {% endif %} 
        {% elif  entity_typ == 'X'  %} 
         select
       NULL AS VALUE,
NULL AS CAMPAIGN_ID,
NULL AS _1_D_VIEW,
NULL AS DATE,
NULL AS _FIVETRAN_ID,
NULL AS INDEX,
NULL AS ACTION_TYPE,
NULL AS _7_D_CLICK,

        '{{ schema_nm }}' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM dual     

    {% endif %}
{% endfor %}