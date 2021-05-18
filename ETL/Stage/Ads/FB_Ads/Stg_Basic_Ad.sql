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
        {{ dbt_utils.surrogate_key('AD_ID') }}  AS BASIC_ID,
AD_ID,
CPM,
DATE,
ADSET_NAME,
AD_NAME,
SPEND,
_FIVETRAN_ID,
FREQUENCY,
REACH,
CPC,
INLINE_LINK_CLICKS,
CTR,
ACCOUNT_ID,
IMPRESSIONS,

        '{{ schema_nm }}' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ schema_nm }}.BASIC_AD
          {% if not loop.last %}
            UNION ALL
        {% endif %} 
        {% elif  entity_typ == 'X'  %} 
         select
        NULL AS AD_ID,
NULL AS CPM,
NULL AS DATE,
NULL AS ADSET_NAME,
NULL AS AD_NAME,
NULL AS SPEND,
NULL AS _FIVETRAN_ID,
NULL AS FREQUENCY,
NULL AS REACH,
NULL AS CPC,
NULL AS INLINE_LINK_CLICKS,
NULL AS CTR,
NULL AS ACCOUNT_ID,
NULL AS IMPRESSIONS,

        '{{ schema_nm }}' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM dual     

    {% endif %}
{% endfor %}