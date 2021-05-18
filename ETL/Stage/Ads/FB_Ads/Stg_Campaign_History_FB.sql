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
        {{ dbt_utils.surrogate_key('ID') }}  AS CAMP_ID,
        START_TIME,
        ID as CAMPAIGN_ID,
        BUDGET_REBALANCE_FLAG,
        SOURCE_CAMPAIGN_ID,
        CONFIGURED_STATUS,
        OBJECTIVE,
        STATUS,
        DAILY_BUDGET,
        BUYING_TYPE,
        NAME,
        CAN_USE_SPEND_CAP,
        EFFECTIVE_STATUS,
        BOOSTED_OBJECT_ID,
        ACCOUNT_ID,
        CREATED_TIME,
        STOP_TIME,
        CAN_CREATE_BRAND_LIFT_STUDY,
        SPEND_CAP,
        UPDATED_TIME,
        _FIVETRAN_SYNCED,
        '{{ schema_nm }}' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ schema_nm }}.CAMPAIGN_HISTORY
          {% if not loop.last %}
            UNION ALL
        {% endif %} 
        {% elif  entity_typ == 'X'  %} 
         select
        Null as ID,
        Null as START_TIME,
        Null as CAMPAIGN_ID,
        Null as BUDGET_REBALANCE_FLAG,
        Null as SOURCE_CAMPAIGN_ID,
        Null as CONFIGURED_STATUS,
        Null as OBJECTIVE,
        Null as STATUS,
        Null as DAILY_BUDGET,
        Null as BUYING_TYPE,
        Null as NAME,
        Null as CAN_USE_SPEND_CAP,
        Null as EFFECTIVE_STATUS,
        Null as BOOSTED_OBJECT_ID,
        Null as ACCOUNT_ID,
        Null as CREATED_TIME,
        Null as STOP_TIME,
        Null as CAN_CREATE_BRAND_LIFT_STUDY,
        Null as SPEND_CAP,
        Null as UPDATED_TIME,
        '{{ schema_nm }}' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM dual     

    {% endif %}
{% endfor %}