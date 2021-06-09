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
        {{ dbt_utils.surrogate_key('ADSET_ID') }}  AS ID,
        CAMPAIGN_NAME,
CPM,
ACCOUNT_ID,
INLINE_LINK_CLICKS,
CTR,
SPEND,
IMPRESSIONS,
DATE,
REACH,
_FIVETRAN_ID,
ADSET_NAME,
CPC,
ADSET_ID,
FREQUENCY,

        '{{ schema_nm }}' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ schema_nm }}.BASIC_AD_SET
          {% if not loop.last %}
            UNION ALL
        {% endif %} 
        {% elif  entity_typ == 'X'  %} 
         select
        NULL AS CAMPAIGN_NAME,
NULL AS CPM,
NULL AS ACCOUNT_ID,
NULL AS INLINE_LINK_CLICKS,
NULL AS CTR,
NULL AS SPEND,
NULL AS IMPRESSIONS,
NULL AS DATE,
NULL AS REACH,
NULL AS _FIVETRAN_ID,
NULL AS ADSET_NAME,
NULL AS CPC,
NULL AS ADSET_ID,
NULL AS FREQUENCY,

        '{{ schema_nm }}' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM dual     

    {% endif %}
{% endfor %}