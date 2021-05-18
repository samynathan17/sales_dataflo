{% set results = get_column_values_from_query("select * from " ~ var('V_DB') ~ "." ~ var('V_Entity_Schema')~ "." ~ var('V_Mkt')~ " where DATASOURCE_TYPE = 'LI_ADS' and READY_TO_PROCESS = 'TRUE'", "ENTITY_DATASORUCE_NAME||'#'||DATASOURCE_TYPE")%}

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

 {% if  entity_typ == 'LI_ADS'  %} 
      
  select
        {{ dbt_utils.surrogate_key('ID') }}  AS ACC_ID,
NOTIFIED_ON_CREATIVE_REJECTION,
TYPE,
NOTIFIED_ON_CREATIVE_APPROVAL,
ID,
LAST_MODIFIED_TIME,
NAME,
CREATED_TIME,
NOTIFIED_ON_CAMPAIGN_OPTIMIZATION,
NOTIFIED_ON_END_OF_CAMPAIGN,
STATUS,
REFERENCE,
CURRENCY,
VERSION_TAG,

        '{{ schema_nm }}' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ schema_nm }}.ACCOUNT_HISTORY
          {% if not loop.last %}
            UNION ALL
        {% endif %} 
        {% elif  entity_typ == 'X'  %} 
         select
       NULL AS NOTIFIED_ON_CREATIVE_REJECTION,
       NULL AS TYPE,
       NULL AS NOTIFIED_ON_CREATIVE_APPROVAL,
       NULL AS ID,
       NULL AS LAST_MODIFIED_TIME,
       NULL AS NAME,
       NULL AS CREATED_TIME,
       NULL AS NOTIFIED_ON_CAMPAIGN_OPTIMIZATION,
       NULL AS NOTIFIED_ON_END_OF_CAMPAIGN,
       NULL AS STATUS,
       NULL AS REFERENCE,
       NULL AS CURRENCY,
       NULL AS VERSION_TAG,

        '{{ schema_nm }}' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM dual     

    {% endif %}
{% endfor %}
