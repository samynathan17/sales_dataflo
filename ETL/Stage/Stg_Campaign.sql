{% set results = get_column_values_from_query("select * from " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".DIM_SALES_ENTITY where ENTITY_TYPE = 'SF'", 'ENTITY_NAME')%}

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
        unique_key= 'Campaign_ID'
      )
}}

{% for V_SF_Schema in results %}
{% if  V_SF_Schema[0:2] == 'SF'  %}

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
        '{{ V_SF_Schema }}' as Source_type,
        'D_CAMPAIGN_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ V_SF_Schema }}.Campaign
        {% if not loop.last %}
            UNION ALL
        {% endif %}  
     {% endif %}

{% endfor %}