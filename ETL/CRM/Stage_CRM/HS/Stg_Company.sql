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
        unique_key= 'Account_ID',
        post_hook="DELETE FROM " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".STG_COMPANY WHERE ACCOUNT_ID IS NULL"
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
        {{ dbt_utils.surrogate_key('id') }}  AS Account_ID,
        ID as Source_ID,
        PORTAL_ID,
        IS_DELETED,
        _FIVETRAN_SYNCED,
        PROPERTY_NUM_ASSOCIATED_CONTACTS,
        PROPERTY_ZIP,
        PROPERTY_HS_ANALYTICS_FIRST_TIMESTAMP,
        PROPERTY_STATE,
        PROPERTY_HS_NUM_CONTACTS_WITH_BUYING_ROLES,
        PROPERTY_NAME,
        PROPERTY_LINKEDINBIO,
        PROPERTY_HS_LASTMODIFIEDDATE,
        PROPERTY_FOUNDED_YEAR,
        PROPERTY_COUNTRY,
        PROPERTY_TIMEZONE,
        PROPERTY_TOTAL_MONEY_RAISED,
        PROPERTY_CITY,
        PROPERTY_HS_ANALYTICS_SOURCE_DATA_2,
        PROPERTY_HS_ANALYTICS_NUM_PAGE_VIEWS,
        PROPERTY_HS_ANALYTICS_SOURCE_DATA_1,
        PROPERTY_HS_NUM_DECISION_MAKERS,
        PROPERTY_HS_NUM_OPEN_DEALS,
        PROPERTY_DOMAIN,
        PROPERTY_HS_NUM_BLOCKERS,
        PROPERTY_WEBSITE,
        PROPERTY_FACEBOOK_COMPANY_PAGE,
        PROPERTY_HS_ANALYTICS_NUM_VISITS,
        PROPERTY_HS_ANALYTICS_SOURCE,
        PROPERTY_ADDRESS,
        PROPERTY_LINKEDIN_COMPANY_PAGE,
        PROPERTY_HS_TOTAL_DEAL_VALUE,
        PROPERTY_HS_TARGET_ACCOUNT_PROBABILITY,
        PROPERTY_IS_PUBLIC,
        PROPERTY_TWITTERHANDLE,
        PROPERTY_PHONE,
        PROPERTY_HUBSPOT_OWNER_ASSIGNEDDATE,
        PROPERTY_HUBSPOT_OWNER_ID,
        PROPERTY_HS_ALL_OWNER_IDS,
        PROPERTY_ADDRESS_2,
        PROPERTY_DESCRIPTION,
        PROPERTY_CREATEDATE,
        PROPERTY_INDUSTRY,
        PROPERTY_ANNUALREVENUE,
        PROPERTY_FIRST_CONTACT_CREATEDATE,
        PROPERTY_WEB_TECHNOLOGIES,
        PROPERTY_NUMBEROFEMPLOYEES,
        PROPERTY_HS_NUM_CHILD_COMPANIES,
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
        'D_COMPANY_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS    
    FROM {{ schema_nm }}.Company
     {% if not loop.last %}
            UNION ALL
        {% endif %}
    {% elif  entity_typ == 'X'  %}     
       select
        null as Account_ID,
        null as Source_ID,
        null as PORTAL_ID,
        null as IS_DELETED,
        null as _FIVETRAN_SYNCED,
        null as PROPERTY_NUM_ASSOCIATED_CONTACTS,
        null as PROPERTY_ZIP,
        null as PROPERTY_HS_ANALYTICS_FIRST_TIMESTAMP,
        null as PROPERTY_STATE,
        null as PROPERTY_HS_NUM_CONTACTS_WITH_BUYING_ROLES,
        null as PROPERTY_NAME,
        null as PROPERTY_LINKEDINBIO,
        null as PROPERTY_HS_LASTMODIFIEDDATE,
        null as PROPERTY_FOUNDED_YEAR,
        null as PROPERTY_COUNTRY,
        null as PROPERTY_TIMEZONE,
        null as PROPERTY_TOTAL_MONEY_RAISED,
        null as PROPERTY_CITY,
        null as PROPERTY_HS_ANALYTICS_SOURCE_DATA_2,
        null as PROPERTY_HS_ANALYTICS_NUM_PAGE_VIEWS,
        null as PROPERTY_HS_ANALYTICS_SOURCE_DATA_1,
        null as PROPERTY_HS_NUM_DECISION_MAKERS,
        null as PROPERTY_HS_NUM_OPEN_DEALS,
        null as PROPERTY_DOMAIN,
        null as PROPERTY_HS_NUM_BLOCKERS,
        null as PROPERTY_WEBSITE,
        null as PROPERTY_FACEBOOK_COMPANY_PAGE,
        null as PROPERTY_HS_ANALYTICS_NUM_VISITS,
        null as PROPERTY_HS_ANALYTICS_SOURCE,
        null as PROPERTY_ADDRESS,
        null as PROPERTY_LINKEDIN_COMPANY_PAGE,
        null as PROPERTY_HS_TOTAL_DEAL_VALUE,
        null as PROPERTY_HS_TARGET_ACCOUNT_PROBABILITY,
        null as PROPERTY_IS_PUBLIC,
        null as PROPERTY_TWITTERHANDLE,
        null as PROPERTY_PHONE,
        null as PROPERTY_HUBSPOT_OWNER_ASSIGNEDDATE,
        null as PROPERTY_HUBSPOT_OWNER_ID,
        null as PROPERTY_HS_ALL_OWNER_IDS,
        null as PROPERTY_ADDRESS_2,
        null as PROPERTY_DESCRIPTION,
        null as PROPERTY_CREATEDATE,
        null as PROPERTY_INDUSTRY,
        null as PROPERTY_ANNUALREVENUE,
        null as PROPERTY_FIRST_CONTACT_CREATEDATE,
        null as PROPERTY_WEB_TECHNOLOGIES,
        null as PROPERTY_NUMBEROFEMPLOYEES,
        null as PROPERTY_HS_NUM_CHILD_COMPANIES,
        null as Source_type,
        null as DW_SESSION_NM,
        null as DW_INS_UPD_DTS,
        null as CUSTOMER_TEXT_1,
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

