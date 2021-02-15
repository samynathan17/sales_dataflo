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
        unique_key= 'User_Role_id'
      )
}}

{% for V_SF_Schema in results %}
         {% if  V_SF_Schema[0:2] == 'SF'  %}

select
        {{ dbt_utils.surrogate_key('id') }}  AS User_Role_id,
        ID as Source_ID,
        NAME,
        PARENT_ROLE_ID,
        ROLLUP_DESCRIPTION,
        OPPORTUNITY_ACCESS_FOR_ACCOUNT_OWNER,
        CASE_ACCESS_FOR_ACCOUNT_OWNER,
        CONTACT_ACCESS_FOR_ACCOUNT_OWNER,
        FORECAST_USER_ID,
        MAY_FORECAST_MANAGER_SHARE,
        LAST_MODIFIED_DATE,
        LAST_MODIFIED_BY_ID,
        SYSTEM_MODSTAMP,
        DEVELOPER_NAME,
        PORTAL_ACCOUNT_ID,
        PORTAL_TYPE,
        PORTAL_ACCOUNT_OWNER_ID,
        '{{ V_SF_Schema }}' as Source_type,
        'D_USER_ROLE_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ V_SF_Schema }}.user_role
            {% if not loop.last %}
               UNION ALL
            {% endif %}  
        {% endif %}

{% endfor %}