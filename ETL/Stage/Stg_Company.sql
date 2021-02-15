{% set results = get_column_values_from_query("select * from " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".DIM_SALES_ENTITY where ENTITY_TYPE = 'HS'", 'ENTITY_NAME')%}

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
        unique_key= 'Account_ID'
      )
}}

{% for V_SF_Schema in results %}
 {% if  V_SF_Schema[0:2] == 'HS'  %} 
 
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
        PROPERTY_HS_UPDATED_BY_USER_ID,
        PROPERTY_HS_CREATED_BY_USER_ID,
        PROPERTY_HUBSPOT_OWNER_ASSIGNEDDATE,
        PROPERTY_HUBSPOT_OWNER_ID,
        PROPERTY_HS_USER_IDS_OF_ALL_OWNERS,
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
        PROPERTY_TYPE,
        '{{ V_SF_Schema }}' as Source_type,
        'D_COMPANY_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS    
    FROM {{ V_SF_Schema }}.Company
      
    {% endif %}
{% endfor %}

