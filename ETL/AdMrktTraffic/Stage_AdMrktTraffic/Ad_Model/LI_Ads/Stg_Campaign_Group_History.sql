{% set results = get_column_values_from_query("select * from " ~ var('V_DB') ~ "." ~ var('V_Entity_Schema')~ "." ~ var('V_Mkt')~" where DATASOURCE_TYPE = 'LI_ADS' and READY_TO_PROCESS = 'TRUE'", "ENTITY_DATASORUCE_NAME||'#'||DATASOURCE_TYPE")%}

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
        {{ dbt_utils.surrogate_key('ID') }}  AS CAMP_GRP_ID,
        BACKFILLED,
        STATUS,
        LAST_MODIFIED_TIME,
        RUN_SCHEDULE_START,
        ACCOUNT_ID,
        ID,
        NAME,
        RUN_SCHEDULE_END,
        CREATED_TIME,
        '{{ schema_nm }}' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ schema_nm }}.CAMPAIGN_GROUP_HISTORY
          {% if not loop.last %}
            UNION ALL
        {% endif %} 
        {% elif  entity_typ == 'X'  %} 
         select
NULL AS BACKFILLED,
NULL AS STATUS,
NULL AS LAST_MODIFIED_TIME,
NULL AS RUN_SCHEDULE_START,
NULL AS ACCOUNT_ID,
NULL AS ID,
NULL AS NAME,
NULL AS RUN_SCHEDULE_END,
NULL AS CREATED_TIME,

        '{{ schema_nm }}' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM dual     

    {% endif %}
{% endfor %}