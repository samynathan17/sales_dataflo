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
        {{ dbt_utils.surrogate_key('ID') }}  AS AD_ID,
       CONFIGURED_STATUS,
BID_INFO_ACTIONS,
ACCOUNT_ID,
STATUS,
LAST_UPDATED_BY_APP_ID,
CREATED_TIME,
EFFECTIVE_STATUS,
CREATIVE_ID,
NAME,
UPDATED_TIME,
AD_SET_ID,
BID_AMOUNT,
AD_SOURCE_ID,
BID_TYPE,
ID,
CAMPAIGN_ID,
_FIVETRAN_SYNCED,
        '{{ schema_nm }}' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ schema_nm }}.AD_HISTORY
          {% if not loop.last %}
            UNION ALL
        {% endif %} 
        {% elif  entity_typ == 'X'  %} 
         select
    NULL AS CONFIGURED_STATUS,
NULL AS BID_INFO_ACTIONS,
NULL AS ACCOUNT_ID,
NULL AS STATUS,
NULL AS LAST_UPDATED_BY_APP_ID,
NULL AS CREATED_TIME,
NULL AS EFFECTIVE_STATUS,
NULL AS CREATIVE_ID,
NULL AS NAME,
NULL AS UPDATED_TIME,
NULL AS AD_SET_ID,
NULL AS BID_AMOUNT,
NULL AS AD_SOURCE_ID,
NULL AS BID_TYPE,
NULL AS ID,
NULL AS CAMPAIGN_ID,
        '{{ schema_nm }}' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM dual     

    {% endif %}
{% endfor %}