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
        unique_key= 'DEAL_ID',
        post_hook="DELETE FROM " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".STG_DEAL WHERE DEAL_ID IS NULL"
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
        cast ({{ dbt_utils.surrogate_key('DEAL_ID') }}  as varchar(100)) AS DEAL_ID,
        DEAL_ID as Source_DEAL_ID,
        PORTAL_ID,
        IS_DELETED,
        _FIVETRAN_SYNCED,
        PROPERTY_HS_DEAL_STAGE_PROBABILITY,
        PROPERTY_HS_CLOSED_AMOUNT_IN_HOME_CURRENCY,
        PROPERTY_HS_LASTMODIFIEDDATE,
        PROPERTY_HS_CLOSED_AMOUNT,
        PROPERTY_HS_TIME_IN_APPOINTMENTSCHEDULED,
        PROPERTY_HS_IS_CLOSED,
        PROPERTY_DAYS_TO_CLOSE,
        PROPERTY_HS_DATE_ENTERED_APPOINTMENTSCHEDULED,
        PROPERTY_HS_PROJECTED_AMOUNT,
        PROPERTY_HS_PROJECTED_AMOUNT_IN_HOME_CURRENCY,
        PROPERTY_AMOUNT_IN_HOME_CURRENCY,
        PROPERTY_HS_DATE_EXITED_DECISIONMAKERBOUGHTIN,
        PROPERTY_HS_DATE_ENTERED_DECISIONMAKERBOUGHTIN,
        PROPERTY_HS_DATE_EXITED_CONTRACTSENT,
        PROPERTY_HS_DATE_EXITED_APPOINTMENTSCHEDULED,
        PROPERTY_HS_DATE_ENTERED_PRESENTATIONSCHEDULED,
        PROPERTY_HS_TIME_IN_QUALIFIEDTOBUY,
        PROPERTY_HS_TIME_IN_CLOSEDWON,
        PROPERTY_HS_TIME_IN_DECISIONMAKERBOUGHTIN,
        PROPERTY_HS_DATE_EXITED_PRESENTATIONSCHEDULED,
        PROPERTY_HS_DATE_ENTERED_QUALIFIEDTOBUY,
        PROPERTY_HS_DATE_EXITED_QUALIFIEDTOBUY,
        PROPERTY_HS_DATE_ENTERED_CONTRACTSENT,
        PROPERTY_HS_TIME_IN_PRESENTATIONSCHEDULED,
        PROPERTY_HS_DATE_ENTERED_CLOSEDWON,
        PROPERTY_HS_TIME_IN_CONTRACTSENT,
        PROPERTY_DEALNAME,
        PROPERTY_CLOSEDATE,
        PROPERTY_CREATEDATE,
        DEAL_PIPELINE_STAGE_ID,
        OWNER_ID,
        PROPERTY_HS_CREATEDATE,
        PROPERTY_HS_ALL_OWNER_IDS,
        DEAL_PIPELINE_ID,
        PROPERTY_HUBSPOT_OWNER_ASSIGNEDDATE,
        PROPERTY_AMOUNT,
         null AS CUSTOMER_TEXT_1,
        NULL AS CUSTOMER_TEXT_2,
        NULL AS CUSTOMER_TEXT_3,
        NULL AS CUSTOMER_TEXT_4,
        NULL AS CUSTOMER_TEXT_5,
        NULL AS CUSTOMER_TEXT_6,
        NULL AS CUSTOMER_NUMBER_1,
        NULL AS CUSTOMER_NUMBER_2,
        NULL AS CUSTOMER_NUMBER_3,
        NULL AS CUSTOMER_DATE_1,
        NULL AS CUSTOMER_DATE_2,
        NULL AS CUSTOMER_DATE_3,   
        '{{ schema_nm }}' as Source_type,
        'D_DEAL_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS    
    FROM {{ schema_nm }}.Deal
        {% if not loop.last %}
            UNION ALL
        {% endif %}
    {% elif  entity_typ == 'X'  %}     
       select
        null as  DEAL_ID,
        null as Source_DEAL_ID,
        null as PORTAL_ID,
        null as IS_DELETED,
        null as _FIVETRAN_SYNCED,
        null as PROPERTY_HS_DEAL_STAGE_PROBABILITY,
        null as PROPERTY_HS_CLOSED_AMOUNT_IN_HOME_CURRENCY,
        null as PROPERTY_HS_LASTMODIFIEDDATE,
        null as PROPERTY_HS_CLOSED_AMOUNT,
        null as PROPERTY_HS_TIME_IN_APPOINTMENTSCHEDULED,
        null as PROPERTY_HS_IS_CLOSED,
        null as PROPERTY_DAYS_TO_CLOSE,
        null as PROPERTY_HS_DATE_ENTERED_APPOINTMENTSCHEDULED,
        null as PROPERTY_HS_PROJECTED_AMOUNT,
        null as PROPERTY_HS_PROJECTED_AMOUNT_IN_HOME_CURRENCY,
        null as PROPERTY_AMOUNT_IN_HOME_CURRENCY,
        null as PROPERTY_HS_DATE_EXITED_DECISIONMAKERBOUGHTIN,
        null as PROPERTY_HS_DATE_ENTERED_DECISIONMAKERBOUGHTIN,
        null as PROPERTY_HS_DATE_EXITED_CONTRACTSENT,
        null as PROPERTY_HS_DATE_EXITED_APPOINTMENTSCHEDULED,
        null as PROPERTY_HS_DATE_ENTERED_PRESENTATIONSCHEDULED,
        null as PROPERTY_HS_TIME_IN_QUALIFIEDTOBUY,
        null as PROPERTY_HS_TIME_IN_CLOSEDWON,
        null as PROPERTY_HS_TIME_IN_DECISIONMAKERBOUGHTIN,
        null as PROPERTY_HS_DATE_EXITED_PRESENTATIONSCHEDULED,
        null as PROPERTY_HS_DATE_ENTERED_QUALIFIEDTOBUY,
        null as PROPERTY_HS_DATE_EXITED_QUALIFIEDTOBUY,
        null as PROPERTY_HS_DATE_ENTERED_CONTRACTSENT,
        null as PROPERTY_HS_TIME_IN_PRESENTATIONSCHEDULED,
        null as PROPERTY_HS_DATE_ENTERED_CLOSEDWON,
        null as PROPERTY_HS_TIME_IN_CONTRACTSENT,
        null as PROPERTY_DEALNAME,
        null as PROPERTY_CLOSEDATE,
        null as PROPERTY_CREATEDATE,
        null as DEAL_PIPELINE_STAGE_ID,
        null as OWNER_ID,
        null as PROPERTY_HS_CREATEDATE,
        null as PROPERTY_HS_ALL_OWNER_IDS,
        null as DEAL_PIPELINE_ID,
        null as PROPERTY_HUBSPOT_OWNER_ASSIGNEDDATE,
        null as PROPERTY_AMOUNT,   
        null as Source_type,
        null as DW_SESSION_NM,
        null as DW_INS_UPD_DTS,
         null AS CUSTOMER_TEXT_1,
        NULL AS CUSTOMER_TEXT_2,
        NULL AS CUSTOMER_TEXT_3,
        NULL AS CUSTOMER_TEXT_4,
        NULL AS CUSTOMER_TEXT_5,
        NULL AS CUSTOMER_TEXT_6,
        NULL AS CUSTOMER_NUMBER_1,
        NULL AS CUSTOMER_NUMBER_2,
        NULL AS CUSTOMER_NUMBER_3,
        NULL AS CUSTOMER_DATE_1,
        NULL AS CUSTOMER_DATE_2,
        NULL AS CUSTOMER_DATE_3   
    FROM dual
    {% endif %}
{% endfor %} 

