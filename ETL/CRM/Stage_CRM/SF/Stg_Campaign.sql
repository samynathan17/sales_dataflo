{% set results = get_column_values_from_query("select * from " ~ var('V_DB') ~ "." ~ var('V_Entity_Schema')~ "." ~ var('V_Sales')~" where DATASOURCE_TYPE = 'SF' and READY_TO_PROCESS = 'TRUE'", "ENTITY_DATASORUCE_NAME||'#'||DATASOURCE_TYPE")%}

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
        unique_key= 'Campaign_ID',
        post_hook="DELETE FROM " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".STG_CAMPAIGN WHERE Campaign_ID IS NULL"
      )
}}

{% for V_SF_Schema in results %}

{% if V_SF_Schema != 'X' %} 
{% set schema_nm,entity_typ = V_SF_Schema.split('#') %}
{% else %}
{% set entity_typ = 'X' %}
{% endif %}

 {% if  entity_typ == 'SF'  %}   
 
 select
        {{ dbt_utils.surrogate_key('id') }}  AS Campaign_ID,
        ID as Source_ID,
        IS_DELETED,
        NAME,
        TYPE,
        STATUS,
        START_DATE,
        END_DATE,
        EXPECTED_REVENUE,
        BUDGETED_COST,
        ACTUAL_COST,
        EXPECTED_RESPONSE,
        NUMBER_SENT,
        IS_ACTIVE,
        DESCRIPTION,
        NUMBER_OF_LEADS,
        NUMBER_OF_CONVERTED_LEADS,
        NUMBER_OF_CONTACTS,
        NUMBER_OF_RESPONSES,
        NUMBER_OF_OPPORTUNITIES,
        NUMBER_OF_WON_OPPORTUNITIES,
        AMOUNT_ALL_OPPORTUNITIES,
        AMOUNT_WON_OPPORTUNITIES,
        OWNER_ID,
        CREATED_DATE,
        CREATED_BY_ID,
        LAST_MODIFIED_DATE,
        LAST_MODIFIED_BY_ID,
        SYSTEM_MODSTAMP,
        LAST_ACTIVITY_DATE,
        LAST_VIEWED_DATE,
        LAST_REFERENCED_DATE,
        CAMPAIGN_MEMBER_RECORD_TYPE_ID,
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
        'D_CAMPAIGN_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ schema_nm }}.Campaign
        {% if not loop.last %}
            UNION ALL
        {% endif %}  
    {% elif  entity_typ == 'X'  %}     
       select
        null as Campaign_ID,
        null as  Source_ID,
        null as IS_DELETED,
        null as NAME,
        null as TYPE,
        null as STATUS,
        null as START_DATE,
        null as END_DATE,
        null as EXPECTED_REVENUE,
        null as BUDGETED_COST,
        null as ACTUAL_COST,
        null as EXPECTED_RESPONSE,
        null as NUMBER_SENT,
        null as IS_ACTIVE,
        null as DESCRIPTION,
        null as NUMBER_OF_LEADS,
        null as NUMBER_OF_CONVERTED_LEADS,
        null as NUMBER_OF_CONTACTS,
        null as NUMBER_OF_RESPONSES,
        null as NUMBER_OF_OPPORTUNITIES,
        null as NUMBER_OF_WON_OPPORTUNITIES,
        null as AMOUNT_ALL_OPPORTUNITIES,
        null as AMOUNT_WON_OPPORTUNITIES,
        null as OWNER_ID,
        null as CREATED_DATE,
        null as CREATED_BY_ID,
        null as LAST_MODIFIED_DATE,
        null as LAST_MODIFIED_BY_ID,
        null as SYSTEM_MODSTAMP,
        null as LAST_ACTIVITY_DATE,
        null as LAST_VIEWED_DATE,
        null as LAST_REFERENCED_DATE,
        null as CAMPAIGN_MEMBER_RECORD_TYPE_ID,
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